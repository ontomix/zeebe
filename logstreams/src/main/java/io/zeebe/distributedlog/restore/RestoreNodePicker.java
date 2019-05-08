package io.zeebe.distributedlog.restore;

import io.atomix.cluster.MemberId;
import java.util.concurrent.CompletableFuture;

public interface RestoreNodePicker {
  CompletableFuture<MemberId> pick();
}
