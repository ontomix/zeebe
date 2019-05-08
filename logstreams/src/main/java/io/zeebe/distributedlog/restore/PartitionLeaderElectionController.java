package io.zeebe.distributedlog.restore;

import io.atomix.cluster.MemberId;
import java.util.concurrent.CompletableFuture;

public interface PartitionLeaderElectionController {

  /**
   * Withdraws from the leader election, stepping down if it is the leader.
   *
   * @return a future which completes when the node has withdrawn from the election primitive
   */
  CompletableFuture<Void> withdraw();

  /**
   * Rejoins the leader election primitive.
   *
   * @return a future which completes when the node has joined the election primitive
   */
  CompletableFuture<Void> join();

  /** @return the current elected leader, or null if none yet elected */
  MemberId getLeader();
}
