package io.zeebe.distributedlog.impl;

import io.atomix.cluster.MemberId;
import io.atomix.core.Atomix;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import java.util.concurrent.CompletableFuture;

public class LogstreamReplicator implements Service<Void> {
   private MemberId leader;
   private Atomix atomix;

   private final Injector<Atomix> atomixInjector = new Injector<>();

  public LogstreamReplicator(MemberId leader) {
    this.leader = leader;
  }

  public Injector<Atomix> getAtomixInjector() {
    return atomixInjector;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    atomix = atomixInjector.getValue();

  }

  private void replicate(){
    final CompletableFuture<LogstreamReplicationResponse> responseFuture = atomix.getCommunicationService()
      .send("replicatelogstream", new LogstreamReplicationRequest(), leader);

    atomix.getCommunicationService().subscribe()
  }

  @Override
  public Void get() {
    return null;
  }
}
