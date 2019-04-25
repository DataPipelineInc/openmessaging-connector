package io.openmessaging.connector.runtime.rest.storage;

import io.openmessaging.connector.runtime.TargetState;
import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.distributed.ClusterStateConfig;
import io.openmessaging.connector.runtime.rest.listener.ConfigListener;
import java.util.List;
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
   * @param connector the name of the connector.
   * @param taskConfig the new configuartion of the task.
   */
  void putTaskConfig(String connector, List<Map<String, String>> taskConfig);

  /**
   * Remove the configuration of an existing connector.
   *
   * @param connectorName the name of the connector.
   */
  void removeConnectorConfig(String connectorName);

  /**
   * Remove the configuration of an existing task.
   *
   * @param connectorName the name of the connector.
   */
  void removeTaskConfig(String connectorName);

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

  /**
   * Get configuration information and target state information of all connectors and tasks in the
   * current cluster.
   *
   * @return Current cluster snapshot information.
   */
  ClusterStateConfig snapshot();

  /**
   * Change the state of the given connector to the given state.
   *
   * @param connector the name of the given connector.
   * @param targetState the state the connector is to change.
   */
  void putTargetState(String connector, TargetState targetState);
}
