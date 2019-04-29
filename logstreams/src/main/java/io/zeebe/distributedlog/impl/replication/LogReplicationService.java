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
package io.zeebe.distributedlog.impl.replication;

import static io.zeebe.logstreams.impl.log.fs.FsLogSegmentDescriptor.METADATA_LENGTH;

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.core.Atomix;
import io.atomix.utils.serializer.Serializer;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogSegment;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.agrona.ExpandableArrayBuffer;
import org.slf4j.LoggerFactory;

public class LogReplicationService implements Service<Void> {
  private final Injector<LogStream> logStreamInjector = new Injector<>();
  private final Injector<Atomix> atomixInjector = new Injector<>();

  private final Serializer serializer = Serializer.using(LogReplicationNameSpace.LOG_NAME_SPACE);

  private LogStream logStream;
  public LogStorage logStorage;
  private ClusterCommunicationService communicationService;

  @Override
  public Void get() {
    return null;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    logStream = logStreamInjector.getValue();
    logStorage = logStream.getLogStorage();
    communicationService = atomixInjector.getValue().getCommunicationService();

    communicationService.subscribe(
        "log.replication.manifest." + logStream.getPartitionId(),
        serializer::decode,
        this::handleManifestRequest,
        (Function<LogReplicationManifestResponse, byte[]>) serializer::encode);
    communicationService.subscribe(
        "log.replication.file." + logStream.getPartitionId(),
        serializer::decode,
        this::handleFileRequest,
        (Function<LogReplicationSegmentResponse, byte[]>) serializer::encode);
  }

  public CompletableFuture<LogReplicationManifestResponse> handleManifestRequest(
      LogReplicationManifestRequest request) {
    final LogReplicationManifestResponse response = new LogReplicationManifestResponse();
    final LogSegment[] segments = logStorage.getSegments();
    response.segments = Arrays.stream(segments).map(LogSegment::id).collect(Collectors.toList());

    return CompletableFuture.completedFuture(response);
  }

  public CompletableFuture<LogReplicationSegmentResponse> handleFileRequest(
      LogReplicationSegmentRequest request) {
    final LogSegment segment = logStorage.getSegment(request.id);
    final ExpandableArrayBuffer dest = new ExpandableArrayBuffer(1024 * 1024);
    final LogReplicationSegmentResponse response = new LogReplicationSegmentResponse();

    int offset = 0;
    int bytesRead =
        segment.readBytes(
            ByteBuffer.wrap(dest.byteArray(), offset, 1024 * 1024), METADATA_LENGTH + offset);
    while (bytesRead > 0) {
      offset += bytesRead;
      dest.checkLimit(offset + (1024 * 1024));
      bytesRead =
          segment.readBytes(
              ByteBuffer.wrap(dest.byteArray(), offset, 1024 * 1024), METADATA_LENGTH + offset);
    }

    response.data = dest.byteArray();
    LoggerFactory.getLogger("LogReplicationService").info("Replicating {} bytes", response.data.length);
    return CompletableFuture.completedFuture(response);
  }

  public Injector<Atomix> getAtomixInjector() {
    return atomixInjector;
  }

  public Injector<LogStream> getLogStreamInjector() {
    return logStreamInjector;
  }
}
