package io.zeebe.logstreams.spi;

import java.nio.ByteBuffer;

public interface LogSegment {
  int id();

  int readBytes(ByteBuffer readBuffer, int offset);

  long size();
}
