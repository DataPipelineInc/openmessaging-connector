package io.openmessaging.connector.runtime.storage.standalone;

import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.rest.storage.PositionStorageService;

import java.util.Collection;
import java.util.Map;

public class MemoryPositionStorage implements PositionStorageService {
  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  @Override
  public void initialize(WorkerConfig workerConfig) {

  }

  @Override
  public void set(Map<byte[], byte[]> values) {

  }

  @Override
  public Map<byte[], byte[]> get(Collection<byte[]> key) {
    return null;
  }
}
