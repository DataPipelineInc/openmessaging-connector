package io.openmessaging.connector.runtime.rest.storage;

import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.utils.CallBack;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * PositionStorageService is an interface for bulk storage of key-value pairs.Users can save
 * position information to memory or save it where they need it. Usually the key is the queue and
 * partition information, and the value is the location information. It only needs to support
 * reading/writing bytes.
 */
public interface PositionStorageService {

  /** Start the position storage service. */
  void start();

  /** Stop the position storage service. */
  void stop();

  /**
   * Config the position storage service with the given config.
   *
   * @param workerConfig the config of worker.
   */
  void initialize(WorkerConfig workerConfig);

  /**
   * Save position to memory or persistence.
   *
   * @param values The key of the map represents partition and the value represents position.
   * @param callBack the callback after saving position
   */
  Future set(Map<ByteBuffer, ByteBuffer> values, CallBack<Void> callBack);

  /**
   * Get position from memory or persistence.
   *
   * @param key binary form of partition.
   * @param callBack the callback after getting position
   */
  void get(Collection<ByteBuffer> key, CallBack<Map<ByteBuffer, ByteBuffer>> callBack);
}
