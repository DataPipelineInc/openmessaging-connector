package io.openmessaging.connector.runtime.rest.storage;

import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.listener.ConfigListener;

import java.util.Map;

/**
 * ConfigStorageService is used to add, update, delete the configuration information of the
 * Connector or task.Users can save configuration information to memory or save it where they need
 * it.
 */
public interface ConfigStorageService {
  /** Start the position storage service. */
  void start();
  /** Stop the position storage service. */
  void stop();

  /**
   * Config the config storage service with the given config.
   *
   * @param workerConfig the config of worker.
   */
  void initialize(WorkerConfig workerConfig);

  /**
   * Create or update the configuration of the given connector.
   *
   * @param connectorName the name of the connector.
   * @param connectorconfig the new configuration of the connector
   */
  void putConnectorConfig(String connectorName, Map<String, String> connectorconfig);

  /**
   * Create or update the configuration of the given task.
   *
   * @param taskId the id of the task.
   * @param taskConfig the new configuartion of the task.
   */
  void putTaskConfig(ConnectorTaskId taskId, Map<String, String> taskConfig);

  /**
   * Remove the configuration of an existing connector.
   *
   * @param connectorName the name of the connector.
   */
  void removeConnectorConfig(String connectorName);

  /**
   * Remove the configuration of an existing task.
   *
   * @param taskId the id of the task.
   */
  void removeTaskConfig(ConnectorTaskId taskId);

  /**
   * Check if the connector configuration exists.
   *
   * @param connectorName the name of the connector being checked.
   * @return true if the configuration of the connector exists.
   */
  boolean contains(String connectorName);

  /**
   * Set a listener to capture configuration changes.The config listener will do something if the
   * configuration has been changed.
   *
   * @param listener the config change listener.
   */
  void setConfigListener(ConfigListener listener);
}
