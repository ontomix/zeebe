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
import io.zeebe.distributedlog.impl.replication.LogReplicationManifestRequest;
import io.zeebe.distributedlog.impl.replication.LogReplicationManifestResponse;
import io.zeebe.distributedlog.impl.replication.LogReplicationNameSpace;
import io.zeebe.distributedlog.impl.replication.LogReplicationSegmentRequest;
import io.zeebe.distributedlog.impl.replication.LogReplicationSegmentResponse;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.util.ZbLogger;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class LogstreamReplicator implements Service<Void> {
  private MemberId leader;
  private Atomix atomix;
  private final int partitionId;

  private static final ZbLogger LOG = new ZbLogger(LogstreamReplicator.class);

  private final Serializer serializer = Serializer.using(LogReplicationNameSpace.LOG_NAME_SPACE);

  private final Injector<Atomix> atomixInjector = new Injector<>();
  private final LogStorage logStorage;

  public LogstreamReplicator(MemberId leader, int partitionId, LogStorage logStorage) {
    this.leader = leader;
    this.partitionId = partitionId;
    this.logStorage = logStorage;
  }

  public Injector<Atomix> getAtomixInjector() {
    return atomixInjector;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    atomix = atomixInjector.getValue();
    final CompletableActorFuture<Void> startFuture = new CompletableActorFuture<>();
    startContext.async(startFuture);
    sendManifestRequest()
        .whenComplete(
            (r, e) -> {
              if (e == null) {
                handleManifestResponse(r);
                startFuture.complete(null);
              } else {
                startFuture.completeExceptionally(e);
              }
            });
  }

  private CompletableFuture<LogReplicationManifestResponse> sendManifestRequest() {
    return atomix
        .getCommunicationService()
        .send(
            "log.replication.manifest." + partitionId,
            new LogReplicationManifestRequest(),
            serializer::encode,
            serializer::decode,
            leader);
  }

  private void handleManifestResponse(LogReplicationManifestResponse manifest) {
    manifest.segments.forEach(
        file ->
            sendReplicationFileRequest(file)
                .whenComplete(
                    (r, e) -> {
                      LOG.info("Received segment {}, error {}", file, e);
                      handleReplicationFileResponse(r);
                    })
                .join());
  }

  private CompletableFuture<LogReplicationSegmentResponse> sendReplicationFileRequest(
      int segmentId) {

    final LogReplicationSegmentRequest request = new LogReplicationSegmentRequest();
    request.id = segmentId;
    LOG.info("Sending request for segmentId {} to node {}", segmentId, leader);
    return atomix
        .getCommunicationService()
        .send(
            "log.replication.file." + partitionId,
            request,
            serializer::encode,
            serializer::decode,
            leader,
            Duration.ofSeconds(60));
  }

  private void handleReplicationFileResponse(LogReplicationSegmentResponse response) {
    // write to files or logstorage

    final long append = logStorage.append(ByteBuffer.wrap(response.data));
    if(append >= 0) {
      LOG.info("Append success");
    }
    else {
      LOG.info("Append failed , returned {}", append);
    }
  }

  @Override
  public Void get() {
    return null;
  }
}
