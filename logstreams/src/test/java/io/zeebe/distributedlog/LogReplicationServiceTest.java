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

import io.zeebe.distributedlog.impl.replication.LogReplicationSegmentRequest;
import io.zeebe.distributedlog.impl.replication.LogReplicationService;
import io.zeebe.logstreams.util.LogStreamRule;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

public class LogReplicationServiceTest {
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  public LogStreamRule logStream = new LogStreamRule(temporaryFolder);

  @Rule public RuleChain ruleChain = RuleChain.outerRule(temporaryFolder).around(logStream);

  @Test
  public void shouldReturnFile() {
    final byte[] buffer = new byte[1024 * 1024 * 100];
    ThreadLocalRandom.current().nextBytes(buffer);
    final LogReplicationService service = new LogReplicationService();
    logStream.getLogStorage().append(ByteBuffer.wrap(buffer));
    service.logStorage = logStream.getLogStorage();
    final LogReplicationSegmentRequest request = new LogReplicationSegmentRequest();
    request.id = 0;

    service
        .handleFileRequest(request)
        .thenAccept(
            r -> {
              LoggerFactory.getLogger("test").info("Test {}", r);
            });
  }
}
