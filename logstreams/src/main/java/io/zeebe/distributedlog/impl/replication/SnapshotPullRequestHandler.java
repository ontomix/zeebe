package io.zeebe.distributedlog.impl.replication;

import io.zeebe.logstreams.state.ReplicationController;
import io.zeebe.logstreams.state.StateStorage;
import io.zeebe.util.ZbLogger;
import java.io.File;
import java.util.List;
import java.util.function.Consumer;

public class SnapshotPullRequestHandler {
  private static final ZbLogger LOG = new ZbLogger(SnapshotPullRequestHandler.class);
  private final StateStorage storage;
  private final ReplicationController replicationController;

  public SnapshotPullRequestHandler(
      StateStorage storage, ReplicationController replicationController) {
    this.storage = storage;
    this.replicationController = replicationController;
  }

  public void replicateSnapshot(long requiredSnapshotPosition, Consumer<Runnable> executor) {
    //TODO: find the snapshot for position requiredSnapshotPosition
    final List<File> snapshots = storage.listByPositionAsc();

    if (snapshots != null && !snapshots.isEmpty()) {
      final File oldestSnapshotDirectory = snapshots.get(0);
       LOG.debug("Start replicating latest snapshot {}", oldestSnapshotDirectory.toPath());

      // TODO: check if snapshotPosition == requiredSnapshotPosition
      final long snapshotPosition = Long.parseLong(oldestSnapshotDirectory.getName());

      final File[] files = oldestSnapshotDirectory.listFiles();
      for (File snapshotChunkFile : files) {
        executor.accept(
            () -> {
              // LOG.debug("Replicate snapshot chunk {}", snapshotChunkFile.toPath());
              replicationController.replicate(snapshotPosition, files.length, snapshotChunkFile);
            });
      }
    }
  }
}
