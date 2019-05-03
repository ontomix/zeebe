package io.zeebe.distributedlog.restore;

import io.atomix.cluster.MemberId;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.zeebe.logstreams.impl.LoggedEventImpl;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStreamReader;
import java.nio.ByteBuffer;
import java.util.HashMap;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class RestoreServer implements AutoCloseable {
  private final RestoreContext context;
  private final ThreadContext threadContext;
  private final ByteBuffer eventsBuffer;
  private LogStreamReader logStreamReader;

  public RestoreServer(RestoreContext context) {
    this.context = context;
    this.threadContext =
        new SingleThreadContext(
            String.format("restore-server-%s-%%d", context.getLogStream().getLogName()));
    this.eventsBuffer = ByteBuffer.allocate(2 * 1024 * 1024);
  }

  public void start() {
    logStreamReader = new BufferedLogStreamReader(context.getLogStream());
    context
        .getCommunicationService()
        .subscribe(context.getRestoreRequestTopic(), this::onRestoreRequest, threadContext);
    context
        .getCommunicationService()
        .subscribe(context.getEventRequestTopic(), this::onEventsRequested, threadContext);
  }

  @Override
  public void close() {
    context.getCommunicationService().unsubscribe(context.getRestoreRequestTopic());
    context.getCommunicationService().unsubscribe(context.getEventRequestTopic());
  }

  public void onRestoreRequest(MemberId requester, HashMap<String, Object> request) {
    final long fromPosition = (Long) request.get("fromPosition");
    final long toPosition = (Long) request.get("toPosition");
    final String restoreRequestTopic = (String) request.get("restoreRequestTopic");
    final boolean eventExists;

    if (fromPosition == -1) {
      logStreamReader.seekToFirstEvent();
      eventExists = true;
    } else {
      eventExists = logStreamReader.seek(fromPosition);
    }

    if (eventExists && logStreamReader.hasNext()) {
      final HashMap<String, Object> response = new HashMap<>();
      final long startAvailablePosition = logStreamReader.next().getPosition();
      final long endAvailablePosition = getEndAvailablePosition(toPosition, startAvailablePosition);

      response.put("fromPosition", startAvailablePosition);
      response.put("toPosition", endAvailablePosition);

      context
          .getLogger()
          .debug(
              "Notify sender that we can provide events from {} to {}",
              startAvailablePosition,
              endAvailablePosition);
      context.getCommunicationService().unicast(restoreRequestTopic, response, requester);
    } else {
      context.getLogger().debug("No events after position {} found", fromPosition);
    }
  }

  public HashMap<String, Object> onEventsRequested(HashMap<String, Object> request) {
    final HashMap<String, Object> response = new HashMap<>();
    final long from = (long) request.get("fromPosition");
    final long to = (long) request.get("toPosition");
    eventsBuffer.clear();

    if (logStreamReader.seek(from) && logStreamReader.hasNext()) {
      final long lastReadPosition = copyAsMuchAsPossible(from);

      response.put("fromPosition", from);
      response.put("toPosition", lastReadPosition);
      response.put("data", eventsBuffer.array());

      context
          .getLogger()
          .debug("Notify sender that we can provide events from {} to {}", from, lastReadPosition);

      return response;
    } else {
      context.getLogger().debug("No events after position {} found", from);
      throw new IllegalStateException(
          String.format(
              "Cannot replicate events '%d' - '%d', no events found after position '%d'",
              from, to, from));
    }
  }

  private long getEndAvailablePosition(long toPosition, long endAvailablePosition) {
    while (logStreamReader.hasNext()) {
      final long position = logStreamReader.next().getPosition();
      if (position > toPosition) {
        break;
      }
      endAvailablePosition = position;
    }
    return endAvailablePosition;
  }

  private long copyAsMuchAsPossible(long fromPosition) {
    final MutableDirectBuffer bufferWrapper = new UnsafeBuffer(eventsBuffer);
    long lastReadPosition = fromPosition;

    do {
      final LoggedEventImpl event = (LoggedEventImpl) logStreamReader.next();
      if (eventsBuffer.remaining() < event.getLength()) {
        break;
      }
      lastReadPosition = event.getPosition();
      event.write(bufferWrapper, eventsBuffer.position());
      eventsBuffer.position(eventsBuffer.position() + event.getLength());
    } while (logStreamReader.hasNext());
    return lastReadPosition;
  }
}
