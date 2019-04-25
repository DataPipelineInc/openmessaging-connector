package io.openmessaging.connector.runtime.storage;

import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.rest.error.ConnectException;
import io.openmessaging.connector.runtime.rest.storage.PositionStorageService;
import io.openmessaging.connector.runtime.utils.CallBack;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This writer holds key-value data.The framework will asynchronously flush these data */
public class PositionStorageWriter {
  private static final Logger log = LoggerFactory.getLogger(PositionStorageWriter.class);
  private Map<ByteBuffer, ByteBuffer> data;
  private Map<ByteBuffer, ByteBuffer> toFlush;
  private WorkerConfig workerConfig;
  private boolean isFlushing;
  private PositionStorageService positionStorageService;

  public PositionStorageWriter(
      WorkerConfig workerConfig, PositionStorageService positionStorageService) {
    this.data = new HashMap<>();
    this.workerConfig = workerConfig;
    this.positionStorageService = positionStorageService;
  }

  /**
   * Set a position for a partition using message values.
   *
   * @param partition the partition to store an position for.
   * @param position the position.
   */
  public synchronized void position(ByteBuffer partition, ByteBuffer position) {
    data.put(partition, position);
  }

  /**
   * The first step of flush operation, this will take a snapshot of the current state.
   *
   * @return true if successfully init, false otherwise.
   */
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

  /**
   * Flush all the data in the current snapshot and clear them after success. This operation is an
   * asynchronous operation, leaving all the data to the positionStorageService for storage.
   *
   * @param callBack the callBack
   * @return a future.
   */
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

  /**
   * Cancel this flush, if there is an error in the flush process, this method will be called to
   * handle the flush failure.
   */
  private synchronized void cancelFlush() {
    if (!toFlush.isEmpty()) {
      toFlush.putAll(data);
      data = toFlush;
      toFlush = null;
    }
  }

  /** This method will be called if flush is successful. */
  private synchronized void onSuccessful() {
    log.info("Successfully flushed {} messages", toFlush.size());
    toFlush = null;
  }

  /** This method will be called if flush is failed. */
  public synchronized void onFailed() {
    cancelFlush();
  }
}
