/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.broker.logstreams;

import io.atomix.cluster.AtomixCluster;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.utils.serializer.Serializer;
import io.zeebe.broker.logstreams.state.StateReplication;
import io.zeebe.distributedlog.impl.replication.LogReplicationNameSpace;
import io.zeebe.distributedlog.impl.replication.RestoreController;
import io.zeebe.logstreams.impl.LogEntryDescriptor;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.logstreams.state.StateStorage;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.util.ZbLogger;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class LogstreamRestorationRequestService implements Service<Void> {
  private MemberId leader;
  private AtomixCluster atomix;
  private final int partitionId;
  private final long toPosition;
  private final long fromPosition;
  private final CompletableActorFuture<Void> startFuture = new CompletableActorFuture<>();

  private static final ZbLogger LOG = new ZbLogger(LogstreamRestorationRequestService.class);

  private final Serializer serializer = Serializer.using(LogReplicationNameSpace.LOG_NAME_SPACE);

  private final Injector<AtomixCluster> atomixInjector = new Injector<>();
  private final LogStorage logStorage;
  private ClusterCommunicationService communicationService;

  private final DirectBuffer readBuffer = new UnsafeBuffer(0, 0);
  private final Injector<StateStorage> stateStorageInjector = new Injector<>();

  public LogstreamRestorationRequestService(
      MemberId leader, int partitionId, LogStorage logStorage, long fromPosition, long toPosition) {
    this.leader = leader;
    this.partitionId = partitionId;
    this.logStorage = logStorage;
    this.fromPosition = fromPosition;
    this.toPosition = toPosition;
  }

  public Injector<AtomixCluster> getAtomixInjector() {
    return atomixInjector;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    atomix = atomixInjector.getValue();
    communicationService = atomix.getCommunicationService();
    startContext.async(startFuture);
    sendRequest(fromPosition, toPosition).whenComplete(this::handleResponse);
  }

  public void handleEventResponse(
      long fromPosition, long toPosition, byte[] bytes, Throwable error) {
    final DirectBuffer buffer = new UnsafeBuffer(bytes);
    int offset = 0;
    long position = LogEntryDescriptor.getPosition(buffer, offset);

    while (position <= fromPosition) {
      offset += LogEntryDescriptor.getFragmentLength(buffer, offset);
      position = LogEntryDescriptor.getPosition(buffer, offset);
    }

    final ByteBuffer data = ByteBuffer.wrap(bytes, offset, bytes.length - offset);

    final long append = logStorage.append(data);
    if (append >= 0) {
      LOG.info(
          "Appended {} (skipping position {})",
          LogEntryDescriptor.getPosition(buffer, offset),
          fromPosition);
      if (toPosition < toPosition) {
        LOG.info("Requesting again {} - {}", toPosition, toPosition);
        // sendRequest(response.toPosition, toPosition).whenComplete(this::handleResponse);
      } else {
        LOG.info("Requested all of {} - {}", fromPosition, toPosition);
        startFuture.complete(null);
      }
    } else {
      LOG.info("Append failed , returned {}", append, error);
      // startFuture.completeExceptionally(error);
    }
  }

  private CompletableFuture<HashMap<String, Object>> sendRequest(
      long fromPosition, long toPosition) {
    final HashMap<String, Object> request = new HashMap<>();
    request.put("fromPosition", fromPosition);
    request.put("toPosition", toPosition);
    return atomix.getCommunicationService().send("log.replication." + partitionId, request, leader);
  }

  @Override
  public Void get() {
    return null;
  }

  public void handleResponse(HashMap<String, Object> response, Throwable e) {
    if(e == null) {
      LOG.error("Couldn't send request");
      return;
    }

    long startAvailablePosition = (Long) response.get("fromPosition");
    if (startAvailablePosition > fromPosition) {
      Long availableSnapshotPosition = (Long) response.get("snapshotPosition");
      replicateSnapshot(availableSnapshotPosition);
    } else {
      long endAvailablePosition = (Long) response.get("toPosition");
      // send event Request
    }
  }

  public CompletableFuture<Void> replicateSnapshot(long availableSnapshotPosition) {
    final CompletableFuture<Void> restoreFuture = new CompletableFuture<>();

    String snapshotReplicationTopic =
      String.format("replication-%d-restore-%d", partitionId, availableSnapshotPosition);
    MemberId sender = leader; // TODO: for now assume you always send the request to the leader
    final HashMap<String, Object> snapshotRequest = generateRequestSnapshot(
      availableSnapshotPosition, restoreFuture);
    atomixInjector.getValue().getCommunicationService().unicast(snapshotReplicationTopic, snapshotRequest, sender);
    return restoreFuture;
  }

  public HashMap<String, Object> generateRequestSnapshot(long availableSnapshotPosition,
    CompletableFuture<Void> snapshotFuture) {
    // Start snapshot replication receiver
    StateReplication replication =
        new StateReplication(
            atomixInjector.getValue().getEventService(),
            partitionId,
            String.format("restore-%d", availableSnapshotPosition));

    final StateStorage storage = stateStorageInjector.getValue();

    RestoreController replicationController = new RestoreController(replication, storage, () -> {});
    final CompletableFuture<Long> restoreFuture =
        replicationController.consumeReplicatedSnapshots();

    restoreFuture.whenComplete(
        (r, e) -> {
          if (e == null) {
            LOG.info(
                "Restored snapshot at position {} in {}",
                availableSnapshotPosition,
                storage.getTmpSnapshotDirectoryFor(Long.toString(availableSnapshotPosition)));
            // continue replicating rest of the events.
            // then mark snapshot as valid
            replicationController.tryToMarkSnapshotAsValid(
                availableSnapshotPosition,
                storage.getTmpSnapshotDirectoryFor(Long.toString(availableSnapshotPosition)),
                storage.getSnapshotDirectoryFor(availableSnapshotPosition));

            replication.close();
            startFuture.complete(null);
            snapshotFuture.complete(null);
          } else {
            // start all over
          }
          // TODO: handle timeout
        });

    // send SnapshotRequest
    HashMap<String, Object> snapshotRequest = new HashMap<>();
    snapshotRequest.put("snapshotPosition", availableSnapshotPosition);
    return snapshotRequest;
  }

  public Injector<StateStorage> getStateStorageInjector() {
    return stateStorageInjector;
  }
}
