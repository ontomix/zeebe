package io.zeebe.distributedlog.impl.replication;

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.core.Atomix;
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
import java.util.stream.Collectors;
import org.agrona.ExpandableArrayBuffer;

public class LogReplicationService implements Service<Void> {
  private final Injector<LogStream> logStreamInjector = new Injector<>();
  private final Injector<Atomix> atomixInjector = new Injector<>();

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
        "log.replication.manifest." + logStream.getPartitionId(), this::handleManifestRequest);
    communicationService.subscribe(
        "log.replication.file." + logStream.getPartitionId(), this::handleFileRequest);
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
}
