package io.openmessaging.connector.runtime.storage.standalone;

import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.rest.storage.PositionStorageService;
import io.openmessaging.connector.runtime.utils.CallBack;
import io.openmessaging.connector.runtime.utils.ConvertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MemoryPositionStorage implements PositionStorageService {
  private static final Logger log = LoggerFactory.getLogger(MemoryPositionStorage.class);
  private Map<ByteBuffer, ByteBuffer> positions;
  private ExecutorService executorService;
  private WorkerConfig workerConfig;

  public MemoryPositionStorage() {
    this.executorService = Executors.newSingleThreadExecutor();
    this.positions = new HashMap<>();
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void initialize(WorkerConfig workerConfig) {
    this.workerConfig = workerConfig;
  }

  @Override
  public Future set(Map<ByteBuffer, ByteBuffer> values, CallBack<Void> callBack) {
    return executorService.submit(
        () -> {
          positions.putAll(values);
          save();
          if (callBack != null) {
            callBack.onCompletion(null, null);
          }
        });
  }

  @Override
  public void get(Collection<ByteBuffer> key, CallBack<Map<ByteBuffer, ByteBuffer>> callBack) {
    executorService.submit(
        () -> {
          Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
          for (Map.Entry<ByteBuffer,ByteBuffer> entry : positions.entrySet()) {
            if (key.contains(entry.getKey())) {
              result.put(entry.getKey(), entry.getValue());
            }
          }
          if (callBack != null) {
            callBack.onCompletion(null, result);
          }
        });
  }

  public void save() {
    for (Map.Entry<ByteBuffer, ByteBuffer> entry : positions.entrySet()) {
      log.info(
          "Partition : {} , Position : {} ",
          new String(entry.getKey().array()),
          new String(entry.getValue().array()));
    }
  }
}
