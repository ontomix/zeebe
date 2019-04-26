package io.zeebe.distributedlog.impl;

import io.atomix.cluster.MemberId;
import io.atomix.core.Atomix;
import io.zeebe.distributedlog.impl.replication.LogReplicationManifestRequest;
import io.zeebe.distributedlog.impl.replication.LogReplicationManifestResponse;
import io.zeebe.distributedlog.impl.replication.LogReplicationSegmentRequest;
import io.zeebe.distributedlog.impl.replication.LogReplicationSegmentResponse;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class LogstreamReplicator implements Service<Void> {
  private MemberId leader;
  private Atomix atomix;
  private final int partitionId;

  private final Injector<Atomix> atomixInjector = new Injector<>();
  private final LogStorage logStorage;

  public LogstreamReplicator(MemberId leader, int partitionId,
    LogStorage logStorage) {
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
    CompletableActorFuture<Void> startFuture = new CompletableActorFuture<>();
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
            "log.replication.manifest." + partitionId, new LogReplicationManifestRequest(), leader);
  }

  private void handleManifestResponse(LogReplicationManifestResponse manifest) {
    manifest.segments.forEach(
        file ->
            sendReplicationFileRequest(file)
                .whenComplete((r, e) -> handleReplicationFileResponse(r))
                .join());
  }

  private CompletableFuture<LogReplicationSegmentResponse> sendReplicationFileRequest(
      int segmentId) {

    final LogReplicationSegmentRequest request = new LogReplicationSegmentRequest();
    request.id = segmentId;
    return atomix
        .getCommunicationService()
        .send("log.replication.file." + partitionId, request, leader);
  }

  private void handleReplicationFileResponse(LogReplicationSegmentResponse response) {
    // write to files or logstorageDefaultDistributedLogstreamService
    logStorage.append(ByteBuffer.wrap(response.data));
  }

  @Override
  public Void get() {
    return null;
  }
}
