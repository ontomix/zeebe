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
package io.zeebe.distributedlog.impl;

import io.atomix.cluster.MemberId;
import io.atomix.core.Atomix;
import io.atomix.utils.serializer.Serializer;
import io.zeebe.distributedlog.impl.replication.LogReplicationNameSpace;
import io.zeebe.distributedlog.impl.replication.LogReplicationRequest;
import io.zeebe.distributedlog.impl.replication.LogReplicationResponse;
import io.zeebe.logstreams.impl.LogEntryDescriptor;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.util.ZbLogger;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class LogstreamReplicator implements Service<Void> {
  private MemberId leader;
  private Atomix atomix;
  private final int partitionId;
  private final long toPosition;
  private final long fromPosition;
  private final CompletableActorFuture<Void> startFuture = new CompletableActorFuture<>();

  private static final ZbLogger LOG = new ZbLogger(LogstreamReplicator.class);

  private final Serializer serializer = Serializer.using(LogReplicationNameSpace.LOG_NAME_SPACE);

  private final Injector<Atomix> atomixInjector = new Injector<>();
  private final LogStorage logStorage;

  public LogstreamReplicator(
      MemberId leader, int partitionId, LogStorage logStorage, long fromPosition, long toPosition) {
    this.leader = leader;
    this.partitionId = partitionId;
    this.logStorage = logStorage;
    this.fromPosition = fromPosition;
    this.toPosition = toPosition;
  }

  public Injector<Atomix> getAtomixInjector() {
    return atomixInjector;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    atomix = atomixInjector.getValue();
    startContext.async(startFuture);
    sendRequest(fromPosition, toPosition).whenComplete(this::handleResponse);
  }

  public void handleResponse(LogReplicationResponse response, Throwable error) {
    final DirectBuffer buffer = new UnsafeBuffer(response.data);
    int offset = 0;
    long position = -1;

    while (position < response.fromPosition) {
      position = LogEntryDescriptor.getPosition(buffer, offset);
      offset += LogEntryDescriptor.getFragmentLength(buffer, offset);
    }
    final ByteBuffer data = ByteBuffer.wrap(response.data, offset, response.data.length - offset);

    final long append = logStorage.append(data);
    if (append >= 0) {
      LOG.info(
          "Appended {} (skipping position {})",
          LogEntryDescriptor.getPosition(buffer, offset),
          response.fromPosition);
      if (response.toPosition < toPosition) {
        LOG.info("Requesting again {} - {}", response.toPosition, toPosition);
        // sendRequest(response.toPosition, toPosition).whenComplete(this::handleResponse);
      } else {
        LOG.info("Requested all of {} - {}", fromPosition, response.toPosition);
        // startFuture.complete(null);
      }
    } else {
      LOG.info("Append failed , returned {}", append, error);
      // startFuture.completeExceptionally(error);
    }
  }

  private CompletableFuture<LogReplicationResponse> sendRequest(
      long fromPosition, long toPosition) {
    final LogReplicationRequest request = new LogReplicationRequest();
    request.fromPosition = fromPosition;
    request.toPosition = toPosition;
    return atomix
        .getCommunicationService()
        .send(
            "log.replication." + partitionId,
            request,
            serializer::encode,
            serializer::decode,
            leader);
  }

  @Override
  public Void get() {
    return null;
  }
}
