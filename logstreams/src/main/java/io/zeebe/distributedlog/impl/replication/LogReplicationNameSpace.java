package io.zeebe.distributedlog.impl.replication;

import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;

/** Storage serializer namespaces. */
public final class LogReplicationNameSpace {

  /** Raft protocol namespace. */
  public static final Namespace LOG_NAME_SPACE =
      Namespace.builder()
          .register(Namespaces.BASIC)
          .register(LogReplicationManifestResponse.class)
          .register(LogReplicationSegmentResponse.class)
          .register(LogReplicationSegmentRequest.class)
          .register(LogReplicationManifestRequest.class)
          .build("RaftProtocol");
}
