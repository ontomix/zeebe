package io.zeebe.broker.logstreams;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.cluster.AtomixCluster;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.utils.net.Address;
import io.zeebe.logstreams.impl.LogStreamBuilder;
import io.zeebe.logstreams.impl.service.LogStreamService;
import io.zeebe.logstreams.state.StateStorage;
import io.zeebe.test.util.AutoCloseableRule;
import io.zeebe.transport.impl.util.SocketUtil;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LogstreamRestorationTest {
  private static final int VALUE = 0xCAFE;
  private static final String KEY = "test";

  @Rule public TemporaryFolder tempFolderRule = new TemporaryFolder();
  @Rule public TemporaryFolder tempFolderRule2 = new TemporaryFolder();
  @Rule public AutoCloseableRule autoCloseableRule = new AutoCloseableRule();

  private AtomixCluster node1;
  private AtomixCluster node2;
  private LogstreamRestorationRequestService requestService;
  private LogStreamRestorationService restoreService;
  private StateStorage receiverStorage;
  private StateStorage snapshotStorage;

  @Before
  public void setup() throws IOException {
    final File runtimeDirectory = tempFolderRule.newFolder("runtime");
    final File snapshotsDirectory = tempFolderRule.newFolder("snapshots");
    snapshotStorage = new StateStorage(runtimeDirectory, snapshotsDirectory);

    final File receiverRuntimeDirectory = tempFolderRule.newFolder("runtime-receiver");
    final File receiverSnapshotsDirectory = tempFolderRule.newFolder("snapshots-receiver");
    receiverStorage = new StateStorage(receiverRuntimeDirectory, receiverSnapshotsDirectory);

    final Address address1 = Address.from(SocketUtil.getNextAddress().port());
    final Address address2 = Address.from(SocketUtil.getNextAddress().port());

    this.node1 =
        AtomixCluster.builder()
            .withMemberId("1")
            .withAddress(address1)
            .withMembershipProvider(
                BootstrapDiscoveryProvider.builder().withNodes(address1, address2).build())
            .build();

    this.node2 =
        AtomixCluster.builder()
            .withMemberId("2")
            .withAddress(address2)
            .withMembershipProvider(
                BootstrapDiscoveryProvider.builder().withNodes(address1, address2).build())
            .build();

    node1.start().join();
    node2.start().join();

    restoreService = new LogStreamRestorationService();
    restoreService.getAtomixInjector().inject(node1);
    restoreService.getLogStreamInjector().inject(new LogStreamService(new LogStreamBuilder(1)));
    restoreService.getStorageInjector().inject(snapshotStorage);
    restoreService.start(null);

    requestService =
        new LogstreamRestorationRequestService(
            node1.getMembershipService().getLocalMember().id(), 1, null, 10, 50);
    requestService.getAtomixInjector().inject(node2);
    requestService.getStateStorageInjector().inject(receiverStorage);
  }

  @Test
  public void shouldReplicateSnapshotChunks() throws IOException {

    long snapshotPosition = 10;
    final File snapshotDirectory = snapshotStorage.getSnapshotDirectoryFor(snapshotPosition);
    snapshotDirectory.mkdir();
    // create 5 chunks for snapshot 10
    for (int i = 0; i < 5; i++) {
      final File newFile = new File(snapshotDirectory, String.valueOf(i));
      newFile.createNewFile();
      new FileWriter(newFile).write("test" + i); // write something;
    }

    final CompletableFuture<Void> restoreFuture = requestService.replicateSnapshot(10);
    restoreFuture.whenComplete(
        (r, e) -> {
          final File tmpSnapshotDirectory =
              receiverStorage.getTmpSnapshotDirectoryFor(String.valueOf(snapshotPosition));
          assertThat(tmpSnapshotDirectory.listFiles().length).isEqualTo(5);
          // TODO mark snapshot as valid
        });
  }
}
