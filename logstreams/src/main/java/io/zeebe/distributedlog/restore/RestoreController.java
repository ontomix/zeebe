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
package io.zeebe.distributedlog.restore;

import io.atomix.cluster.MemberId;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.zeebe.logstreams.impl.LoggedEventImpl;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class RestoreController {
  private final RestoreContext context;
  private final ThreadContext threadContext;

  public RestoreController(RestoreContext context) {
    this.context = context;
    threadContext =
        new SingleThreadContext(
            String.format("restore-server-%s-%%d", context.getLogStream().getLogName()));
  }

  public CompletableFuture<Long> restore(long fromPosition, long toPosition) {
    final RestoreSession session = new RestoreSession(fromPosition, toPosition);
    session.requestRestore();
    return session.result;
  }

  class RestoreSession implements AutoCloseable {
    private final long fromPosition;
    private final long toPosition;
    private final String restoreRequestTopic;
    private final AtomicBoolean sessionStarted;
    private final AtomicBoolean closed;
    private final CompletableFuture<Long> result;

    private long latestPosition;

    RestoreSession(long fromPosition, long toPosition) {
      this.fromPosition = fromPosition;
      this.toPosition = toPosition;
      this.latestPosition = fromPosition;
      this.sessionStarted = new AtomicBoolean();
      this.closed = new AtomicBoolean();
      this.result = new CompletableFuture<>();
      this.restoreRequestTopic =
          String.format("log-restore-session-%s", UUID.randomUUID().toString());
    }

    void requestRestore() {
      final HashMap<String, Object> request = new HashMap<>();
      request.put("fromPosition", latestPosition);
      request.put("toPosition", toPosition);
      request.put("restoreRequestTopic", restoreRequestTopic);

      sessionStarted.set(false);
      context
          .getCommunicationService()
          .subscribe(restoreRequestTopic, this::handleRestoreResponse, threadContext);
      context.getCommunicationService().broadcast(context.getRestoreRequestTopic(), request);

      context
          .getLogger()
          .info(
              "Requested missing events {} - {} (out of {} - {})",
              latestPosition,
              toPosition,
              fromPosition,
              toPosition);

      threadContext.schedule(
          Duration.ofSeconds(5),
          () -> {
            if (closed.get()) {
              return;
            }

            // todo: eventually handle race condition issues with sessionStarted (e.g. make it a
            // counter or something)
            context.getLogger().info("Timed out waiting for restore response, retrying...");
            if (!sessionStarted.get()) {
              requestRestore();
            }
          });
    }

    void handleRestoreResponse(MemberId sender, HashMap<String, Object> response) {
      if (!sessionStarted.compareAndSet(false, true)) {
        context
            .getLogger()
            .info("Ignoring request from {} since restore session was already started", sender);
        return;
      }

      final long fromPosition = (long) response.get("fromPosition");
      final long toPosition = (long) response.get("toPosition");

      if (this.latestPosition > -1 && fromPosition > this.latestPosition) {
        // TODO: handle case where there must be a snapshot then
        context.getLogger().info("Expecting a snapshot to be present in response {}", response);
      } else {
        final CompletableFuture<Long> replicated = new CompletableFuture<>();
        replicateMissingEvents(sender, fromPosition, toPosition, replicated);
        replicated.whenCompleteAsync(
            (lastPosition, error) -> {
              if (error != null) {
                context
                    .getLogger()
                    .info(
                        "Failed to replicate missing events {} - {} from {}, failed at {}",
                        fromPosition,
                        toPosition,
                        sender,
                        lastPosition);
              } else {
                context
                    .getLogger()
                    .info(
                        "Replicated missing events {} - {} from {}",
                        fromPosition,
                        toPosition,
                        sender);
              }

              if (lastPosition < this.toPosition) {
                threadContext.schedule(Duration.ofMillis(100), this::requestRestore);
              } else {
                result.complete(lastPosition);
                closed.set(true);
              }
            },
            threadContext);
      }
    }

    private void replicateMissingEvents(
        MemberId target,
        long fromPosition,
        long toPosition,
        CompletableFuture<Long> replicatedFuture) {
      final HashMap<String, Object> request = new HashMap<>();
      request.put("fromPosition", fromPosition);
      request.put("toPosition", toPosition);

      context
          .getLogger()
          .info("Replicating missing events {} - {} from {}", fromPosition, toPosition, target);
      final CompletableFuture<HashMap<String, Object>> future =
          context
              .getCommunicationService()
              .send(context.getEventRequestTopic(), request, target, Duration.ofSeconds(5));
      future.whenCompleteAsync(
          (r, e) -> {
            if (e != null) {
              context
                  .getLogger()
                  .error(
                      "Failed to replicate events {} - {}",
                      request.get("fromPosition"),
                      request.get("toPosition"));
              replicatedFuture.completeExceptionally(e);
            } else {
              final long newFromPosition = (long) r.get("toPosition");
              final ByteBuffer blockToAppend = skipAlreadyCommittedEvents((byte[]) r.get("data"));

              // todo: handle errors
              final long appendResult =
                  context.getLogStream().getLogStorage().append(blockToAppend);
              if (appendResult > 0) {
                context.getLogStream().setCommitPosition(newFromPosition);
              }

              context
                  .getLogger()
                  .info(
                      "Replicated events {} - {} from {}",
                      r.get("fromPosition"),
                      newFromPosition,
                      target);
              this.latestPosition = newFromPosition;
              if (newFromPosition < toPosition) {
                replicateMissingEvents(target, newFromPosition, toPosition, replicatedFuture);
              } else {
                replicatedFuture.complete(newFromPosition);
              }
            }
          },
          threadContext);
    }

    private ByteBuffer skipAlreadyCommittedEvents(byte[] data) {
      final LoggedEventImpl event = new LoggedEventImpl();
      final DirectBuffer wrapper = new UnsafeBuffer(data);
      int offset = 0;

      do {
        event.wrap(wrapper, offset);
        if (event.getPosition() > context.getLogStream().getCommitPosition()) {
          break;
        } else {
          context.getLogger().info("Skipping event {}", event.getPosition());
        }

        offset += event.getFragmentLength();
      } while (wrapper.capacity() < offset);

      return ByteBuffer.wrap(data, offset, data.length - offset);
    }

    @Override
    public void close() {
      closed.set(true);
      context.getCommunicationService().unsubscribe(restoreRequestTopic);
    }
  }
}
