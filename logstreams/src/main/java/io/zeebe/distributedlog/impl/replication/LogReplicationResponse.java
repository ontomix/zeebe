package io.zeebe.distributedlog.impl.replication;

public class LogReplicationResponse {
  public long fromPosition;
  public long toPosition;
  public byte[] data;
}
