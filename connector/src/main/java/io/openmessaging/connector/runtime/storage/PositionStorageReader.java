package io.openmessaging.connector.runtime.storage;

import java.util.Collection;
import java.util.Map;

public class PositionStorageReader implements io.openmessaging.connector.api.PositionStorageReader {
  @Override
  public byte[] getPosition(byte[] partition) {
    return new byte[0];
  }

  @Override
  public Map<byte[], byte[]> getPositions(Collection<byte[]> partitions) {
    return null;
  }
}
