package io.zeebe.distributedlog.impl;

import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.service.ServiceExecutor;
import io.zeebe.logstreams.log.LogStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestoreController {
  private static final Logger LOG = LoggerFactory.getLogger(RestoreController.class);

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

    private long latestPosition;

    public RestoreSession(long fromPosition, long toPosition) {
      this.fromPosition = fromPosition;
      this.toPosition = toPosition;
      this.latestPosition = fromPosition;
      this.sessionStarted = new AtomicBoolean();
      this.restoreRequestTopic =
          String.format("log-restore-session-%s", UUID.randomUUID().toString());
    }

    void requestRestore() {
      final HashMap<String, Object> request = new HashMap<>();

      sessionStarted.set(false);
      communicationService.subscribe(restoreRequestTopic, this::handleRestoreResponse, executor);
      communicationService.broadcast(getRestoreRequestTopic(), request);

      // TODO: schedule timeout to check and try to broadcast again
      // executor.schedule()
    }

    void handleRestoreResponse(MemberId sender, HashMap<String, Object> response) {
      if (!sessionStarted.compareAndSet(false, true)) {
        LOG.debug("Ignoring request from {} since restore session was already started", sender);
        return;
      }

      final long fromPosition = (long) response.get("fromPosition");
      final long toPosition = (long) response.get("toPosition");

      if (fromPosition > this.latestPosition) {
        // TODO: handle case where there must be a snapshot then
        LOG.debug("Expecting a snapshot to be present in response {}", response);
      } else {
        final CompletableFuture<Long> replicated =
            replicateMissingEvents(sender, fromPosition, toPosition);
        replicated.whenComplete(
            (lastPosition, error) -> {
              if (error != null) {
                LOG.debug(
                    "Failed to replicate missing events {} - {} from {}, failed at {}",
                    fromPosition,
                    toPosition,
                    sender,
                    lastPosition);
                executor.schedule(Duration.ofMillis(100), this::requestRestore);
              }
            });
      }
    }

    private CompletableFuture<Long> replicateMissingEvents(
        MemberId target, long fromPosition, long toPosition) {
      final CompletableFuture<Long> replicatedFuture = new CompletableFuture<>();
      final HashMap<String, Object> request = new HashMap<>();
      request.put("fromPosition", fromPosition);
      request.put("toPosition", toPosition);

      LOG.debug("Replicating missing events {} - {} from {}", fromPosition, toPosition, target);
      final CompletableFuture<HashMap<String, Object>> future =
          communicationService.send(getEventRequestTopic(), request, target, Duration.ofSeconds(5));
      future.whenComplete(
          (r, e) -> {
            if (e != null) {
              LOG.error(
                  "Failed to replicate events {} - {}",
                  request.get("fromPosition"),
                  request.get("toPosition"));
              replicatedFuture.completeExceptionally(e);
            } else {
              final long newFromPosition = (long) r.get("toPosition");
              final byte[] data = (byte[]) r.get("data");

              logStream.getLogStorage().append(ByteBuffer.wrap(data));

              LOG.debug(
                  "Replicated events {} - {} from {}",
                  r.get("fromPosition"),
                  newFromPosition,
                  target);
              this.latestPosition = newFromPosition;
              if (newFromPosition < toPosition) {
                replicateMissingEvents(target, newFromPosition, toPosition);
              } else {
                replicatedFuture.complete(newFromPosition);
              }
            }
          });

      return replicatedFuture;
    }

    @Override
    public void close() throws Exception {
      communicationService.unsubscribe(restoreRequestTopic);
    }

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
