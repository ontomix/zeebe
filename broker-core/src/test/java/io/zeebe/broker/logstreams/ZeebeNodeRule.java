/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.logstreams;

import static io.zeebe.broker.logstreams.LogStreamServiceNames.logStreamRestorationService;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.atomix.cluster.AtomixCluster;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.zeebe.distributedlog.restore.RestoreController;
import io.zeebe.logstreams.LogStreams;
import io.zeebe.logstreams.impl.LogStreamBuilder;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamBatchWriterImpl;
import io.zeebe.logstreams.state.StateStorage;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.servicecontainer.ServiceContainer;
import io.zeebe.servicecontainer.testing.ServiceContainerRule;
import io.zeebe.test.util.TestUtil;
import java.io.IOException;
import java.util.stream.IntStream;
import org.junit.rules.TemporaryFolder;

public class ZeebeNodeRule {
  private final AtomixCluster atomix;
  private final LogStreamBuilder logStreamBuilder;
  private final TemporaryFolder root;
  private final ServiceContainer serviceContainer;
  private final LogStreamRestorationService restorationService;
  private final SingleThreadContext threadContext = new SingleThreadContext("log-restoration-%d");

  private LogStream logStream;
  private RestoreController restoreController;

  public ZeebeNodeRule(
      Node node,
      NodeDiscoveryProvider provider,
      TemporaryFolder root,
      ServiceContainerRule serviceContainer) {
    this.root = root;
    this.atomix =
        AtomixCluster.builder()
            .withAddress(node.address())
            .withMembershipProvider(provider)
            .build();
    this.serviceContainer = serviceContainer.get();
    this.restorationService = new LogStreamRestorationService();

    try {
      this.logStreamBuilder =
          LogStreams.createFsLogStream(1)
              .logDirectory(root.newFolder("log").getAbsolutePath())
              .logSegmentSize(64 * 1024 * 1024)
              .indexBlockSize(4 * 1024 * 1024)
              .logName(node.id().id())
              .serviceContainer(this.serviceContainer)
              .indexStateStorage(new StateStorage(root.newFolder("state").getAbsolutePath()))
              .logName(node.id().id());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public AtomixCluster getAtomix() {
    return atomix;
  }

  public LogStream getLogStream() {
    return logStream;
  }

  public RestoreController getRestoreController() {
    return restoreController;
  }

  public LogStreamRestorationService getRestorationService() {
    return restorationService;
  }

  public long generateEvents(int numberOfEvents) {
    final WorkflowInstanceRecord record = new WorkflowInstanceRecord();
    final LogStreamBatchWriterImpl writer = new LogStreamBatchWriterImpl(logStream);
    writer.wrap(logStream);

    IntStream.range(0, numberOfEvents).forEach(i -> writer.key(i).valueWriter(record).done());
    final long position = writer.tryWrite();
    TestUtil.waitUntil(() -> logStream.getCommitPosition() >= position);

    return position;
  }

  public void start() {
    final Atomix mockedAtomix = mock(Atomix.class);
    when(mockedAtomix.getCommunicationService()).thenReturn(atomix.getCommunicationService());

    atomix.start().join();
    logStream = logStreamBuilder.build().join();
    logStream.openAppender().join();
    restorationService.getLogStreamInjector().inject(logStream);
    restorationService.getAtomixInjector().inject(mockedAtomix);
    restoreController =
        new RestoreController(
            atomix.getCommunicationService(), logStream, threadContext, threadContext);

    serviceContainer
        .createService(logStreamRestorationService(1), restorationService)
        .install()
        .join();
  }

  public void stop() {
    atomix.stop().join();
    logStream.closeAppender().join();
    logStream.close();
    logStream = null;
  }
}
