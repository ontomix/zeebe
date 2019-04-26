package io.zeebe.distributedlog.impl.replication;

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.core.Atomix;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import java.util.concurrent.CompletableFuture;

public class LogReplicationService implements Service<Void> {
  private final Injector<LogStream> logStreamInjector = new Injector<>();
  private final Injector<Atomix> atomixInjector = new Injector<>();

  private LogStream logStream;
  private LogStorage logStorage;
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
        "log.replication.manifest." + logStream.getPartitionId(), this::handleManifestRequest);
    communicationService.subscribe(
        "log.replication.file." + logStream.getPartitionId(), this::handleFileRequest);
  }

  private CompletableFuture<LogReplicationManifestResponse> handleManifestRequest(
      LogReplicationManifestRequest request) {
    return CompletableFuture.completedFuture(null);
  }

  private CompletableFuture<LogReplicationFileResponse> handleFileRequest(
      LogReplicationFileRequest request) {
    return CompletableFuture.completedFuture(null);
  }
}
