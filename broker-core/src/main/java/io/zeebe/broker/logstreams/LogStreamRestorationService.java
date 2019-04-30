package io.zeebe.broker.logstreams;

import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.core.Atomix;
import io.zeebe.logstreams.impl.LoggedEventImpl;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogStreamRestorationService implements Service<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(LogStreamRestorationService.class);
  private final Injector<LogStream> logStreamInjector = new Injector<>();
  private final Injector<Atomix> atomixInjector = new Injector<>();

  private Executor restoreRequestExecutor;
  private Executor eventRequestExecutor;
  private Executor snapshotRequestExecutor;

  private ClusterCommunicationService communicationService;
  private LogStream logStream;

  @Override
  public void start(ServiceStartContext startContext) {
    final Atomix atomix = atomixInjector.getValue();

    if (atomix == null) {
      throw new IllegalStateException("Missing atomix dependency");
    }
    communicationService = atomix.getCommunicationService();

    logStream = logStreamInjector.getValue();
    if (logStream == null) {
      throw new IllegalStateException("Missing log stream dependency");
    }

    createExecutors();
    subscribe();
  }

  @Override
  public void stop(ServiceStopContext stopContext) {
    unsubscribe();
  }

  @Override
  public Void get() {
    return null;
  }

  public Injector<LogStream> getLogStreamInjector() {
    return logStreamInjector;
  }

  public Injector<Atomix> getAtomixInjector() {
    return atomixInjector;
  }

  private void createExecutors() {
    restoreRequestExecutor =
        Executors.newSingleThreadExecutor((r) -> new Thread(r, getRestoreRequestTopic()));
    eventRequestExecutor =
        Executors.newSingleThreadExecutor((r) -> new Thread(r, getEventRequestTopic()));
    snapshotRequestExecutor =
        Executors.newSingleThreadExecutor((r) -> new Thread(r, getSnapshotRequestTopic()));
  }

  private void unsubscribe() {
    communicationService.unsubscribe(getEventRequestTopic());
    communicationService.unsubscribe(getSnapshotRequestTopic());
    communicationService.unsubscribe(getRestoreRequestTopic());
  }

  private void subscribe() {
    // received as broadcast
    communicationService.subscribe(
        getRestoreRequestTopic(), this::handleRestoreRequest, restoreRequestExecutor);

    // received as direct requests
    communicationService.subscribe(
        getEventRequestTopic(), this::handleEventRequest, eventRequestExecutor);
    communicationService.subscribe(
        getSnapshotRequestTopic(), this::handleSnapshotRequest, snapshotRequestExecutor);
  }

  private void handleRestoreRequest(MemberId requester, HashMap<String, Object> request) {
    final LogStreamReader reader = new BufferedLogStreamReader(logStream);
    final long fromPosition = (Long) request.get("fromPosition");
    final long toPosition = (Long) request.get("toPosition");
    final String restoreRequestTopic = (String) request.get("restoreRequestTopic");

    if (reader.seek(fromPosition) && reader.hasNext()) {
      final HashMap<String, Object> response = new HashMap<>();
      final long startAvailablePosition = reader.next().getPosition();
      final long endAvailablePosition =
          getEndAvailablePosition(reader, toPosition, startAvailablePosition);

      response.put("fromPosition", startAvailablePosition);
      response.put("toPosition", endAvailablePosition);

      LOG.debug(
          "Notify sender that we can provide events from {} to {}",
          startAvailablePosition,
          endAvailablePosition);
      communicationService.unicast(restoreRequestTopic, response, requester);
    } else {
      LOG.debug("No events after position {} found", fromPosition);
    }
  }

  private CompletableFuture<HashMap<String, Object>> handleEventRequest(
      HashMap<String, Object> request) {
    final CompletableFuture<HashMap<String, Object>> responseFuture = new CompletableFuture<>();
    final LogStreamReader reader = new BufferedLogStreamReader(logStream);
    final long fromPosition = (Long) request.get("fromPosition");
    final long toPosition = (Long) request.get("toPosition");

    if (reader.seek(fromPosition) && reader.hasNext()) {
      final HashMap<String, Object> response = new HashMap<>();
      final ByteBuffer destination = ByteBuffer.allocate(64 * 1024 * 1024);
      long lastReadPosition = copyAsMuchAsPossible(reader, fromPosition, destination);

      response.put("fromPosition", fromPosition);
      response.put("toPosition", lastReadPosition);
      response.put("data", destination.array());

      LOG.debug(
          "Notify sender that we can provide events from {} to {}", fromPosition, lastReadPosition);
      responseFuture.complete(response);
    } else {
      LOG.debug("No events after position {} found", fromPosition);
      responseFuture.completeExceptionally(
          new IllegalStateException(
              String.format(
                  "Cannot replicate events '%d' - '%d', no events found after position '%d'",
                  fromPosition, toPosition, fromPosition)));
    }

    return responseFuture;
  }

  private CompletableFuture<HashMap<String, Object>> handleSnapshotRequest(
      HashMap<String, Object> request) {
    final HashMap<String, Object> response = new HashMap<>();
    return CompletableFuture.completedFuture(response);
  }

  private long copyAsMuchAsPossible(
      LogStreamReader reader, long fromPosition, ByteBuffer destination) {
    final MutableDirectBuffer bufferWrapper = new UnsafeBuffer(destination);
    long lastReadPosition = fromPosition;

    do {
      final LoggedEventImpl event = (LoggedEventImpl) reader.next();
      if (destination.remaining() < event.getLength()) {
        break;
      }
      lastReadPosition = event.getPosition();
      event.write(bufferWrapper, destination.position());
    } while (reader.hasNext());
    return lastReadPosition;
  }

  private long getEndAvailablePosition(
      LogStreamReader reader, long toPosition, long endAvailablePosition) {
    while (reader.hasNext()) {
      final long position = reader.next().getPosition();
      if (position > toPosition) {
        break;
      }
      endAvailablePosition = position;
    }
    return endAvailablePosition;
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
