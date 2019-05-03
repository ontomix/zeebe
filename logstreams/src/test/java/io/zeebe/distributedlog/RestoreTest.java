package io.zeebe.distributedlog;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.cluster.AtomixCluster;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.zeebe.distributedlog.restore.RestoreContext;
import io.zeebe.distributedlog.restore.RestoreController;
import io.zeebe.distributedlog.restore.RestoreServer;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.util.LogStreamRule;
import io.zeebe.logstreams.util.LogStreamWriterRule;
import io.zeebe.test.util.MsgPackUtil;
import io.zeebe.test.util.TestUtil;
import io.zeebe.transport.impl.util.SocketUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

public class RestoreTest {
  private final Node masterNode =
      Node.builder().withHost("localhost").withPort(SocketUtil.getNextAddress().port()).build();
  private final Node slaveNode =
      Node.builder().withHost("localhost").withPort(SocketUtil.getNextAddress().port()).build();

  private final TemporaryFolder masterTempFolder = new TemporaryFolder();
  private final LogStreamRule masterLogStream = new LogStreamRule(masterTempFolder);
  private final LogStreamWriterRule masterLogStreamWriter =
      new LogStreamWriterRule(masterLogStream);

  @Rule
  public RuleChain masterRuleChain =
      RuleChain.outerRule(masterTempFolder).around(masterLogStream).around(masterLogStreamWriter);

  private final TemporaryFolder slaveTempFolder = new TemporaryFolder();
  private final LogStreamRule slaveLogStream = new LogStreamRule(slaveTempFolder);

  @Rule
  public RuleChain slaveRuleChain = RuleChain.outerRule(slaveTempFolder).around(slaveLogStream);

  private AtomixCluster masterCluster;
  private AtomixCluster slaveCluster;

  @Before
  public void setup() {
    final NodeDiscoveryProvider bootstrap =
        BootstrapDiscoveryProvider.builder().withNodes(masterNode, slaveNode).build();
    masterCluster =
        AtomixCluster.builder()
            .withAddress(masterNode.address())
            .withMembershipProvider(bootstrap)
            .build();
    slaveCluster =
        AtomixCluster.builder()
            .withAddress(slaveNode.address())
            .withMembershipProvider(bootstrap)
            .build();

    masterCluster.start().join();
    slaveCluster.start().join();
  }

  @Test
  public void should() {
    final RestoreContext masterContext =
        new RestoreContext(
            masterCluster.getCommunicationService(),
            masterLogStream.getLogStream(),
            LoggerFactory.getLogger("master"));
    final RestoreContext slaveContext =
        new RestoreContext(
            slaveCluster.getCommunicationService(),
            slaveLogStream.getLogStream(),
            LoggerFactory.getLogger("slave"));
    final RestoreServer master = new RestoreServer(masterContext);
    final RestoreController slave = new RestoreController(slaveContext);
    final long finalPosition =
        masterLogStreamWriter.writeEvents(100000, MsgPackUtil.asMsgPack("{'a':1}"));

    master.start();
    slave
        .restore(-1, finalPosition)
        .whenComplete(
            (p, e) -> {
              System.out.println("Restored until " + p);
            })
        .join();

    TestUtil.waitUntil(() -> slaveLogStream.getLogStream().getCommitPosition() == finalPosition);
    final BufferedLogStreamReader slaveReader =
        new BufferedLogStreamReader(slaveLogStream.getLogStream());

    // for some reason this never reached the 100000th event, but further below it does...?
    //    TestUtil.waitUntil(
    //        () -> {
    //          slaveReader.seekToLastEvent();
    //          if (slaveReader.hasNext()) {
    //            LoggedEvent event = slaveReader.next();
    //            final long latestPosition = event.getPosition();
    //            return latestPosition == finalPosition;
    //          }
    //          return false;
    //        });

    final BufferedLogStreamReader masterReader =
        new BufferedLogStreamReader(masterLogStream.getLogStream());

    masterReader.seekToFirstEvent();
    slaveReader.seekToFirstEvent();
    int count = 1;

    while (masterReader.hasNext()) {
      final LoggedEvent slaveEvent = slaveReader.next();
      final LoggedEvent masterEvent = masterReader.next();
      assertThat(masterEvent.getPosition()).isEqualTo(slaveEvent.getPosition());
      assertThat(slaveEvent.getKey()).isEqualTo(count);
      count++;
    }

    // seems like there's some race condition in there
    assertThat(count).isEqualTo(100001);

    TestUtil.waitUntil(
        () -> {
          slaveReader.seekToLastEvent();
          if (slaveReader.hasNext()) {
            LoggedEvent event = slaveReader.next();
            final long latestPosition = event.getPosition();
            return latestPosition == finalPosition;
          }
          return false;
        });
  }
}
