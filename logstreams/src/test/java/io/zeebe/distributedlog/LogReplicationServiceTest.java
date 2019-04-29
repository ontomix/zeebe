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
package io.zeebe.distributedlog;

import io.zeebe.distributedlog.impl.LogstreamReplicator;
import io.zeebe.distributedlog.impl.replication.LogReplicationRequest;
import io.zeebe.distributedlog.impl.replication.LogReplicationSegmentRequest;
import io.zeebe.distributedlog.impl.replication.LogReplicationService;
import io.zeebe.logstreams.util.LogStreamRule;
import io.zeebe.logstreams.util.LogStreamWriterRule;
import io.zeebe.test.util.MsgPackUtil;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import org.agrona.DirectBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class LogReplicationServiceTest {
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  public LogStreamRule logStream = new LogStreamRule(temporaryFolder);
  public LogStreamWriterRule logStreamWriter = new LogStreamWriterRule(logStream);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(temporaryFolder).around(logStream).around(logStreamWriter);

  @Test
  public void shouldReturnFile() {
    final byte[] buffer = new byte[1024 * 1024 * 100];
    ThreadLocalRandom.current().nextBytes(buffer);
    final LogReplicationService service = new LogReplicationService();
    logStream.getLogStorage().append(ByteBuffer.wrap(buffer));
    final LogReplicationSegmentRequest request = new LogReplicationSegmentRequest();
  }

  @Test
  public void shouldReceiveChunk() {
    final LogReplicationService service = new LogReplicationService();
    final DirectBuffer event = MsgPackUtil.asMsgPack("{'a':1}");
    final LogReplicationRequest request = new LogReplicationRequest();
    request.fromPosition = -1;
    request.toPosition = -1;

    long lastPosition = -1;
    for (int i = 1; i <= 100000; i++) {
      final int key = i;
      lastPosition = logStreamWriter.tryWrite(w -> w.value(event).key(key));

      if (request.fromPosition < 0) {
        request.fromPosition = lastPosition;
      }
      request.toPosition = lastPosition;
    }

    logStreamWriter.waitForPositionToBeAppended(lastPosition);
    request.fromPosition = 4294970176L;
    final LogstreamReplicator replicator =
        new LogstreamReplicator(
            null, 0, logStream.getLogStorage(), request.fromPosition, request.toPosition);
    service.setLogStream(logStream.getLogStream());
    service.handleRequest(request).whenComplete(replicator::handleResponse).join();
  }
}
