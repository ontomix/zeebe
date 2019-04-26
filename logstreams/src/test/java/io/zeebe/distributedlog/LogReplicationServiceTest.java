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
    LogReplicationService service = new LogReplicationService();
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
