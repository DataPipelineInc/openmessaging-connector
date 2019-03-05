package io.openmessaging.connector.runtime.rest.storage;

import io.openmessaging.connector.runtime.WorkerConfig;

import java.util.Collection;
import java.util.Map;

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
   * @param values the map The key of the map represents partition and the value represents.
   *     position.
   */
  void set(Map<byte[], byte[]> values);

  /**
   * Get position from memory or persistence.
   *
   * @param key binary form of partition.
   */
  Map<byte[], byte[]> get(Collection<byte[]> key);
}