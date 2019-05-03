/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.logstreams;

import io.atomix.cluster.AtomixCluster;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.zeebe.broker.logstreams.state.StateReplication;
import io.zeebe.distributedlog.impl.replication.SnapshotPullRequestHandler;
import io.zeebe.logstreams.impl.LoggedEventImpl;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.state.ReplicationController;
import io.zeebe.logstreams.state.StateStorage;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogStreamRestorationService implements Service<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(LogStreamRestorationService.class);
  private final Injector<LogStream> logStreamInjector = new Injector<>();
  private final Injector<AtomixCluster> atomixInjector = new Injector<>();
  private final Injector<StateStorage> storageInjector = new Injector<>();

  private Executor restoreRequestExecutor;
  private Executor eventRequestExecutor;
  private Executor snapshotRequestExecutor;

  private ClusterCommunicationService communicationService;
  private LogStream logStream;
  private StateStorage storage;

  @Override
  public void start(ServiceStartContext startContext) {
    final AtomixCluster atomix = atomixInjector.getValue();

    if (atomix == null) {
      throw new IllegalStateException("Missing atomix dependency");
    }
    communicationService = atomix.getCommunicationService();

    logStream = logStreamInjector.getValue();
    if (logStream == null) {
      throw new IllegalStateException("Missing log stream dependency");
    }

    storage = storageInjector.getValue();

    createExecutors();
    subscribe();
  }

  @Override
  public void stop(ServiceStopContext stopContext) {
    unsubscribe();
  }

  @Override
  public Void get() {
    return null;
  }

  public Injector<LogStream> getLogStreamInjector() {
    return logStreamInjector;
  }

  public Injector<AtomixCluster> getAtomixInjector() {
    return atomixInjector;
  }

  private void createExecutors() {
    restoreRequestExecutor =
        Executors.newSingleThreadExecutor((r) -> new Thread(r, getRestoreRequestTopic()));
    eventRequestExecutor =
        Executors.newSingleThreadExecutor((r) -> new Thread(r, getEventRequestTopic()));
    snapshotRequestExecutor =
        Executors.newSingleThreadExecutor((r) -> new Thread(r, getSnapshotRequestTopic()));
  }

  private void unsubscribe() {
    communicationService.unsubscribe(getEventRequestTopic());
    communicationService.unsubscribe(getSnapshotRequestTopic());
    communicationService.unsubscribe(getRestoreRequestTopic());
  }

  private void subscribe() {
    // received as broadcast
    communicationService.subscribe(
        getRestoreRequestTopic(), this::handleRestoreRequest, restoreRequestExecutor);

    // received as direct requests
    communicationService.subscribe(
        getEventRequestTopic(), this::handleEventRequest, eventRequestExecutor);
    communicationService.subscribe(
        getSnapshotRequestTopic(), this::handleSnapshotRequest, snapshotRequestExecutor);
  }

  private void handleRestoreRequest(MemberId requester, HashMap<String, Object> request) {
    final LogStreamReader reader = new BufferedLogStreamReader(logStream);
    final long fromPosition = (Long) request.get("fromPosition");
    final long toPosition = (Long) request.get("toPosition");
    final String restoreRequestTopic = (String) request.get("restoreRequestTopic");

    if (reader.seek(fromPosition) && reader.hasNext()) {
      final HashMap<String, Object> response = new HashMap<>();
      final long startAvailablePosition = reader.next().getPosition();
      final long endAvailablePosition =
          getEndAvailablePosition(reader, toPosition, startAvailablePosition);

      response.put("fromPosition", startAvailablePosition);
      response.put("toPosition", endAvailablePosition);
      if (startAvailablePosition > fromPosition) {
        response.put("snapshotPosition", getAvailableSnapshotPositionAfter(fromPosition));
      }

      LOG.debug(
          "Notify sender that we can provide events from {} to {}",
          startAvailablePosition,
          endAvailablePosition);
      communicationService.unicast(restoreRequestTopic, response, requester);
    } else {
      LOG.debug("No events after position {} found", fromPosition);
    }
  }

  private long getAvailableSnapshotPositionAfter(long fromPosition) {
    final List<File> snapshots = storage.listByPositionAsc();

    if (snapshots != null && !snapshots.isEmpty()) {
      final File oldestSnapshotDirectory = snapshots.get(0);
      final long snapshotPosition = Long.parseLong(oldestSnapshotDirectory.getName());
      return snapshotPosition;
    } else {
      LOG.error("This case never happens");
      return -1; // snapshot not available
    }
  }

  private CompletableFuture<HashMap<String, Object>> handleEventRequest(
      HashMap<String, Object> request) {
    final CompletableFuture<HashMap<String, Object>> responseFuture = new CompletableFuture<>();
    final LogStreamReader reader = new BufferedLogStreamReader(logStream);
    final long fromPosition = (Long) request.get("fromPosition");
    final long toPosition = (Long) request.get("toPosition");

    if (reader.seek(fromPosition) && reader.hasNext()) {
      final HashMap<String, Object> response = new HashMap<>();
      final ByteBuffer destination = ByteBuffer.allocate(64 * 1024 * 1024);
      final long lastReadPosition = copyAsMuchAsPossible(reader, fromPosition, destination);

      response.put("fromPosition", fromPosition);
      response.put("toPosition", lastReadPosition);
      response.put("data", destination.array());

      LOG.debug(
          "Notify sender that we can provide events from {} to {}", fromPosition, lastReadPosition);
      responseFuture.complete(response);
    } else {
      LOG.debug("No events after position {} found", fromPosition);
      responseFuture.completeExceptionally(
          new IllegalStateException(
              String.format(
                  "Cannot replicate events '%d' - '%d', no events found after position '%d'",
                  fromPosition, toPosition, fromPosition)));
    }

    return responseFuture;
  }

  public CompletableFuture<HashMap<String, Object>> handleSnapshotRequest(
      HashMap<String, Object> request) {
    final HashMap<String, Object> response = new HashMap<>();

    long snapshotPosition = (Long) request.get("snapshotPosition");

    final List<File> snapshots = storage.listByPositionAsc();

    if (snapshots != null && !snapshots.isEmpty()) {

      LOG.debug("Replicating snapshot at pos {}", snapshotPosition);
      StateReplication replication =
          new StateReplication(
              atomixInjector.getValue().getEventService(),
              logStream.getPartitionId(),
              String.format("restore-%d", snapshotPosition));

      ReplicationController replicationController =
          new ReplicationController(replication, storage, () -> {});
      SnapshotPullRequestHandler replicator =
          new SnapshotPullRequestHandler(storage, replicationController);

      replicator.replicateSnapshot(snapshotPosition, snapshotRequestExecutor::execute);
    }
    return CompletableFuture.completedFuture(response);
  }

  private long copyAsMuchAsPossible(
      LogStreamReader reader, long fromPosition, ByteBuffer destination) {
    final MutableDirectBuffer bufferWrapper = new UnsafeBuffer(destination);
    long lastReadPosition = fromPosition;

    do {
      final LoggedEventImpl event = (LoggedEventImpl) reader.next();
      if (destination.remaining() < event.getLength()) {
        break;
      }
      lastReadPosition = event.getPosition();
      event.write(bufferWrapper, destination.position());
    } while (reader.hasNext());
    return lastReadPosition;
  }

  private long getEndAvailablePosition(
      LogStreamReader reader, long toPosition, long endAvailablePosition) {
    while (reader.hasNext()) {
      final long position = reader.next().getPosition();
      if (position > toPosition) {
        break;
      }
      endAvailablePosition = position;
    }
    return endAvailablePosition;
  }

  private String getRestoreRequestTopic() {
    return String.format("log-restore-%d", logStream.getPartitionId());
  }

  private String getEventRequestTopic() {
    return String.format("log-restore-events-%d", logStream.getPartitionId());
  }

  private String getSnapshotRequestTopic() {
    return String.format("log-restore-snapshots-%d", logStream.getPartitionId());
  }

  public Injector<StateStorage> getStorageInjector() {
    return storageInjector;
  }
}
