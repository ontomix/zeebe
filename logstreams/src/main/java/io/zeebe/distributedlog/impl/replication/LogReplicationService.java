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
import io.zeebe.logstreams.impl.CompleteEventsInBlockProcessor;
import io.zeebe.logstreams.impl.log.index.LogBlockIndex;
import io.zeebe.logstreams.impl.log.index.LogBlockIndexContext;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.slf4j.LoggerFactory;

public class LogReplicationService implements Service<Void> {
  private final Injector<LogStream> logStreamInjector = new Injector<>();
  private final Injector<Atomix> atomixInjector = new Injector<>();

  private final CompleteEventsInBlockProcessor completeEventProcessor =
      new CompleteEventsInBlockProcessor();
  private final Serializer serializer = Serializer.using(LogReplicationNameSpace.LOG_NAME_SPACE);

  private LogStream logStream;
  private LogBlockIndexContext logBlockIndexContext;
  private LogBlockIndex logBlockIndex;
  private LogStorage logStorage;
  private ClusterCommunicationService communicationService;

  @Override
  public Void get() {
    return null;
  }

  public void setLogStream(LogStream logStream) {
    this.logStream = logStream;
    logBlockIndex = logStream.getLogBlockIndex();
    logBlockIndexContext = logBlockIndex.createLogBlockIndexContext();
    logStorage = logStream.getLogStorage();
  }

  @Override
  public void start(ServiceStartContext startContext) {
    setLogStream(logStreamInjector.getValue());
    communicationService = atomixInjector.getValue().getCommunicationService();

    communicationService.subscribe(
        "log.replication." + logStream.getPartitionId(),
        serializer::decode,
        this::handleRequest,
        (Function<LogReplicationResponse, byte[]>) serializer::encode);
  }

  public CompletableFuture<LogReplicationResponse> handleRequest(LogReplicationRequest request) {
    final LogReplicationResponse response = new LogReplicationResponse();
    final ByteBuffer buffer = ByteBuffer.allocate(3000);
    final CompletableFuture<LogReplicationResponse> future = new CompletableFuture<>();
    long address = logBlockIndex.lookupBlockAddress(logBlockIndexContext, request.fromPosition);

    if (address < 0) {
      // position not found in index fallback to first block
      address = logStorage.getFirstBlockAddress();
    }

    final long endAddress = logStorage.read(buffer, address, completeEventProcessor);
    if (endAddress > 0) {
      response.fromPosition = request.fromPosition;
      response.toPosition = completeEventProcessor.getLastReadEventPosition();
      response.data = buffer.array();
      LoggerFactory.getLogger("LogReplicationService")
          .info("Replicating {} bytes", response.data.length);
      future.complete(response);
    } else {
      future.completeExceptionally(
          new IllegalStateException(String.format("Got operation result %d", endAddress)));
    }

    return future;
  }

  public Injector<Atomix> getAtomixInjector() {
    return atomixInjector;
  }

  public Injector<LogStream> getLogStreamInjector() {
    return logStreamInjector;
  }
}
