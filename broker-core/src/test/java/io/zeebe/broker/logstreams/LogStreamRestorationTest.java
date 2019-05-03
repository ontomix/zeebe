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

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.zeebe.servicecontainer.testing.ServiceContainerRule;
import io.zeebe.transport.impl.util.SocketUtil;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class LogStreamRestorationTest {
  private final TemporaryFolder masterTempFolder = new TemporaryFolder();
  private final ActorSchedulerRule masterActorScheduler = new ActorSchedulerRule();
  private final ServiceContainerRule masterServiceContainer =
      new ServiceContainerRule(masterActorScheduler);

  @Rule
  public RuleChain masterRule =
      RuleChain.outerRule(masterTempFolder)
          .around(masterActorScheduler)
          .around(masterServiceContainer);

  private final TemporaryFolder slaveTempFolder = new TemporaryFolder();
  private final ActorSchedulerRule slaveActorScheduler = new ActorSchedulerRule();
  private final ServiceContainerRule slaveServiceContainer =
      new ServiceContainerRule(slaveActorScheduler);

  @Rule
  public RuleChain slaveRule =
      RuleChain.outerRule(slaveTempFolder)
          .around(slaveActorScheduler)
          .around(slaveServiceContainer);

  private ZeebeNodeRule master;
  private ZeebeNodeRule slave;

  @Before
  public void setup() {
    final Node masterNode =
        Node.builder().withHost("localhost").withPort(SocketUtil.getNextAddress().port()).build();
    final Node slaveNode =
        Node.builder().withHost("localhost").withPort(SocketUtil.getNextAddress().port()).build();
    final NodeDiscoveryProvider bootstrap =
        BootstrapDiscoveryProvider.builder().withNodes(masterNode, slaveNode).build();

    master = new ZeebeNodeRule(masterNode, bootstrap, masterTempFolder, masterServiceContainer);
    slave = new ZeebeNodeRule(slaveNode, bootstrap, slaveTempFolder, slaveServiceContainer);

    master.start();
    slave.start();
  }

  @Test
  public void should() {
    final long toPosition = master.generateEvents(1000);
    final long fromPosition = -1;

    slave
        .getRestoreController()
        .restore(fromPosition, toPosition)
        .whenComplete(
            (p, e) -> {
              assertThat(p).isEqualTo(toPosition);
              assertThat(e).isNull();
            })
        .join();
  }
}
