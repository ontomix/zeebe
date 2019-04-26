package io.zeebe.distributedlog.impl;

import io.atomix.cluster.MemberId;
import io.atomix.core.Atomix;
import io.zeebe.distributedlog.impl.replication.LogReplicationFileRequest;
import io.zeebe.distributedlog.impl.replication.LogReplicationFileResponse;
import io.zeebe.distributedlog.impl.replication.LogReplicationManifestRequest;
import io.zeebe.distributedlog.impl.replication.LogReplicationManifestResponse;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.util.concurrent.CompletableFuture;

public class LogstreamReplicator implements Service<Void> {
  private MemberId leader;
  private Atomix atomix;

  private final Injector<Atomix> atomixInjector = new Injector<>();

  public LogstreamReplicator(MemberId leader) {
    this.leader = leader;
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
        .send("log.replication.manifest", new LogReplicationManifestRequest(), leader);
  }

  private void handleManifestResponse(LogReplicationManifestResponse manifest) {
    manifest.files.forEach(
        file ->
            sendReplicationFileRequest(file)
                .whenComplete((r, e) -> handleReplicationFileResponse(r))
                .join());
  }

  private CompletableFuture<LogReplicationFileResponse> sendReplicationFileRequest(
      String fileName) {

    final LogReplicationFileRequest request = new LogReplicationFileRequest();
    request.filename = fileName;
    return atomix.getCommunicationService().send("log.replication.file", request, leader);
  }

  private void handleReplicationFileResponse(LogReplicationFileResponse response) {
    // write to files or logstorage
  }

  @Override
  public Void get() {
    return null;
  }
}
