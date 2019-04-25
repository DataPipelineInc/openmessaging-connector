package io.openmessaging.connector.runtime.storage;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

public class PositionStorageReader implements io.openmessaging.connector.api.PositionStorageReader {
  @Override
  public ByteBuffer getPosition(ByteBuffer partition) {
    return ByteBuffer.wrap(new byte[0]);
  }

  @Override
  public Map<ByteBuffer, ByteBuffer> getPositions(Collection<ByteBuffer> partitions) {
    return null;
  }
}
