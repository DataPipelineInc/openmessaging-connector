package io.openmessaging.connector.runtime.storage;

import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.rest.error.ConnectException;
import io.openmessaging.connector.runtime.rest.storage.PositionStorageService;
import io.openmessaging.connector.runtime.utils.CallBack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class PositionStorageWriter {
  private static final Logger log = LoggerFactory.getLogger(PositionStorageWriter.class);
  private Map<ByteBuffer,ByteBuffer> data;
  private Map<ByteBuffer,ByteBuffer> toFlush;
  private WorkerConfig workerConfig;
  private boolean isFlushing;
  private PositionStorageService positionStorageService;

  public PositionStorageWriter(
      WorkerConfig workerConfig, PositionStorageService positionStorageService) {
    this.data = new HashMap<>();
    this.workerConfig = workerConfig;
    this.positionStorageService = positionStorageService;
  }

  public synchronized void position(ByteBuffer partition, ByteBuffer position) {
    data.put(partition, position);
  }

  public synchronized boolean beforeFlush() {
    if (toFlush != null) {
      log.error(
          "Invalid call to PositionStorageWriter flush() while already flushing, the "
              + "framework should not allow this");
      throw new ConnectException("PositionStorageWriter is already flushing");
    }
    if (data.isEmpty()) {
      return false;
    }
    toFlush = data;
    data = new HashMap<>();
    return true;
  }

  public synchronized Future doFlush(CallBack<Void> callBack) {
    return positionStorageService.set(
        toFlush,
        (throwable, result) -> {
          if (throwable != null) {
            onFailed();
          } else {
            onSuccessful();
          }
          if (callBack != null) {
            callBack.onCompletion(throwable, result);
          }
        });
  }

  private synchronized void cancelFlush() {
    if (!toFlush.isEmpty()) {
      toFlush.putAll(data);
      data = toFlush;
      toFlush = null;
    }
  }

  private synchronized void onSuccessful() {
    log.info("Successfully flushed {} messages", toFlush.size());
    toFlush = null;
  }

  public synchronized void onFailed() {
    cancelFlush();
  }
}
