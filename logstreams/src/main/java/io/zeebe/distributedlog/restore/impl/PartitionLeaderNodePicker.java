package io.zeebe.distributedlog.restore.impl;

import io.atomix.cluster.MemberId;
import io.atomix.utils.concurrent.Scheduler;
import io.zeebe.distributedlog.restore.PartitionLeaderElectionController;
import io.zeebe.distributedlog.restore.RestoreNodePicker;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Implements a restore node picking strategy which returns the current elected partition leader; if
 * that is the local node, then an error is returned instead. If it is null (e.g. no leader is yet
 * elected), it simply re-schedules picking forever.
 */
public class PartitionLeaderNodePicker implements RestoreNodePicker {
  private static final String LOCAL_NODE_LEADER_ERROR_MESSAGE =
      "Expected to pick a remote restore node, but the current partition leader is the local node; please withdraw from the election locally first";

  private final PartitionLeaderElectionController electionController;
  private final String localNodeId;
  private final Scheduler scheduler;

  public PartitionLeaderNodePicker(
      PartitionLeaderElectionController electionController,
      String localNodeId,
      Scheduler scheduler) {
    this.electionController = electionController;
    this.localNodeId = localNodeId;
    this.scheduler = scheduler;
  }

  @Override
  public CompletableFuture<MemberId> pick() {
    final CompletableFuture<MemberId> result = new CompletableFuture<>();
    tryPick(result);
    return result;
  }

  private void tryPick(CompletableFuture<MemberId> result) {
    final MemberId leader = electionController.getLeader();
    if (leader == null) {
      scheduler.schedule(Duration.ofMillis(100), () -> tryPick(result));
    } else if (leader.id().equals(localNodeId)) {
      result.completeExceptionally(new IllegalStateException(LOCAL_NODE_LEADER_ERROR_MESSAGE));
    } else {
      result.complete(leader);
    }
  }
}
