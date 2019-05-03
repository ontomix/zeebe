package io.zeebe.distributedlog.restore;

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.zeebe.logstreams.log.LogStream;
import org.slf4j.Logger;

public class RestoreContext {
  private final ClusterCommunicationService communicationService;
  private final LogStream logStream;
  private final Logger logger;

  public RestoreContext(
      ClusterCommunicationService communicationService, LogStream logStream, Logger logger) {
    this.communicationService = communicationService;
    this.logStream = logStream;
    this.logger = logger;
  }

  public ClusterCommunicationService getCommunicationService() {
    return communicationService;
  }

  public LogStream getLogStream() {
    return logStream;
  }

  public Logger getLogger() {
    return logger;
  }

  public String getRestoreRequestTopic() {
    return String.format("log-restore-%d", logStream.getPartitionId());
  }

  public String getEventRequestTopic() {
    return String.format("log-restore-events-%d", logStream.getPartitionId());
  }

  public String getSnapshotRequestTopic() {
    return String.format("log-restore-snapshots-%d", logStream.getPartitionId());
  }
}
