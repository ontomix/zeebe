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

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.core.Atomix;
import io.atomix.utils.serializer.Serializer;
import io.zeebe.logstreams.impl.log.fs.FsLogSegment;
import io.zeebe.logstreams.impl.log.fs.FsLogStorage;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogSegment;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.agrona.ExpandableArrayBuffer;

public class LogReplicationService implements Service<Void> {
  private final Injector<LogStream> logStreamInjector = new Injector<>();
  private final Injector<Atomix> atomixInjector = new Injector<>();

  private final Serializer serializer = Serializer.using(LogReplicationNameSpace.LOG_NAME_SPACE);

  private LogStream logStream;
  private FsLogStorage logStorage;
  private ClusterCommunicationService communicationService;

  @Override
  public Void get() {
    return null;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    logStream = logStreamInjector.getValue();
    logStorage = (FsLogStorage) logStream.getLogStorage();
    communicationService = atomixInjector.getValue().getCommunicationService();

    communicationService.subscribe(
        "log.replication.manifest." + logStream.getPartitionId(), serializer::decode, this::handleManifestRequest,
      (Function<LogReplicationManifestResponse, byte[]>) serializer::encode);
    communicationService.subscribe(
        "log.replication.file." + logStream.getPartitionId(), serializer::decode, this::handleFileRequest,
      (Function<LogReplicationSegmentResponse, byte[]>) serializer::encode);
  }

  private CompletableFuture<LogReplicationManifestResponse> handleManifestRequest(
      LogReplicationManifestRequest request) {
    final LogReplicationManifestResponse response = new LogReplicationManifestResponse();
    final LogSegment[] segments = logStorage.getSegments();
    response.segments = Arrays.stream(segments).map(LogSegment::id).collect(Collectors.toList());

    return CompletableFuture.completedFuture(response);
  }

  private CompletableFuture<LogReplicationSegmentResponse> handleFileRequest(
      LogReplicationSegmentRequest request) {
    final LogSegment segment = logStorage.getSegment(request.id);
    final ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
    final ExpandableArrayBuffer dest = new ExpandableArrayBuffer();
    final LogReplicationSegmentResponse response = new LogReplicationSegmentResponse();

    int offset = 0;
    int bytesRead = segment.readBytes(buffer, offset);

    while (bytesRead != FsLogSegment.END_OF_SEGMENT && bytesRead != FsLogSegment.NO_DATA) {
      offset += bytesRead;
      dest.putBytes(offset, buffer, bytesRead);
      bytesRead = segment.readBytes(buffer, offset);
    }

    response.data = dest.byteArray();
    return CompletableFuture.completedFuture(response);
  }

  public Injector<Atomix> getAtomixInjector() {
    return atomixInjector;
  }

  public Injector<LogStream> getLogStreamInjector() {
    return logStreamInjector;
  }
}
