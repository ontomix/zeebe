package io.zeebe.distributedlog.impl;

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.service.ServiceExecutor;
import io.zeebe.logstreams.log.LogStream;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class RestoreController {
  private final ClusterCommunicationService communicationService;
  private final LogStream logStream;
  private final ServiceExecutor executor;

  public RestoreController(
      ClusterCommunicationService communicationService,
      LogStream logStream,
      ServiceExecutor executor) {
    this.communicationService = communicationService;
    this.logStream = logStream;
    this.executor = executor;
  }

  public CompletableFuture<Long> restore(long fromPosition, long toPosition) {
    final CompletableFuture<Long> future = new CompletableFuture<>();

    return future;
  }

  class RestoreSession implements AutoCloseable {
    private final long fromPosition;
    private final long toPosition;
    private final String restoreRequestTopic;
    private final AtomicBoolean sessionStarted;

    public RestoreSession(long fromPosition, long toPosition) {
      this.fromPosition = fromPosition;
      this.toPosition = toPosition;
      this.sessionStarted = new AtomicBoolean();
      this.restoreRequestTopic =
          String.format("log-restore-session-%s", UUID.randomUUID().toString());
    }

    void requestRestore() {
      final HashMap<String, Object> request = new HashMap<>();
      communicationService.subscribe(restoreRequestTopic, this::handleRestoreResponse, executor);
      communicationService.broadcast(getRestoreRequestTopic(), request);

      // TODO: schedule timeout to check and try to broadcast again
      // executor.schedule()
    }

    void handleRestoreResponse(HashMap<String, Object> response) {}

    private String getRestoreRequestTopic() {
      return String.format("log-restore-%d", logStream.getPartitionId());
    }

    private String getEventRequestTopic() {
      return String.format("log-restore-events-%d", logStream.getPartitionId());
    }

    private String getSnapshotRequestTopic() {
      return String.format("log-restore-snapshots-%d", logStream.getPartitionId());
    }
  }
}
