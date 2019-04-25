package io.openmessaging.connector.runtime;

import io.openmessaging.connector.runtime.rest.entities.ConnectorInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorStateInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.entities.TaskInfo;
import io.openmessaging.connector.runtime.utils.CallBack;
import java.util.List;
import java.util.Map;

/** The main processor of the Connector and Task */
public interface Processor {
  /** Start the processor. */
  void start();

  /** Stop the processor. */
  void stop();

  /**
   * Get the names of all the connectors that are running in the current cluster.
   *
   * @return a list of the names.
   */
  List<String> connectors();

  /**
   * Set the configuration for a connector.
   *
   * @param connectorName the name of the connector.
   * @param config the configuration of the connector.
   * @param callBack a callBack called after the configuration has been written.
   */
  void putConnectorConfig(
      String connectorName, Map<String, String> config, CallBack<ConnectorInfo> callBack);

  /**
   * Set the configuration for a connector.
   *
   * @param connectorName the name of the connector.
   * @param configs a iist of configuration of the task under this connector.
   * @param callBack a callBack called after the configuration has been written.
   */
  void putTaskConfig(
      String connectorName, List<Map<String, String>> configs, CallBack<List<TaskInfo>> callBack);

  /**
   * Get the information of a connector.
   *
   * @param connectorName the name of the connector.
   * @param callBack a callBack called after we get the information.
   */
  void connectorInfo(String connectorName, CallBack<ConnectorInfo> callBack);

  /**
   * Delete a connector and its configuration.
   *
   * @param connector the name of the connector.
   * @param callBack a callBack called after the connector has been deleted.
   */
  void deleteConnectorConfig(String connector, CallBack<ConnectorInfo> callBack);

  /**
   * Get the configuration of the connector.
   *
   * @param connector the name of the connector.
   * @param callBack a callBack called after we get the configuration.
   */
  void connectorConfig(String connector, CallBack<Map<String, String>> callBack);

  /**
   * Get all the configuration of the tasks under this connector.
   *
   * @param connector the name of the connector.
   * @param callBack a callBack called after we get the configuration.
   */
  void taskConfigs(String connector, CallBack<List<TaskInfo>> callBack);

  /**
   * Get the current status of this connector and its tasks.
   *
   * @param connector the name of the connector.
   * @param callBack a callBack called after we get the state information.
   */
  void connectorStatus(String connector, CallBack<ConnectorStateInfo> callBack);

  /**
   * Get the current status of this task.
   *
   * @param taskId the id of the task.
   * @param callBack a callBack called after we get the state information.
   */
  void taskStatus(ConnectorTaskId taskId, CallBack<ConnectorStateInfo.TaskState> callBack);

  /**
   * Restart the connector.
   *
   * @param connector the name of the connector.
   * @param callBack a callBack called after we has restarted this connector.
   */
  void restartConnector(String connector, CallBack<Void> callBack);

  /**
   * Restart the task.
   *
   * @param taskId the id of the task.
   * @param callBack a callBack called after we has restarted this task.
   */
  void restartTask(ConnectorTaskId taskId, CallBack<Void> callBack);

  /**
   * Pause the connector.
   *
   * @param connector the name of the connector.
   */
  void pauseConnector(String connector);

  /**
   * Resume the connector.
   *
   * @param connector the name of the connector.
   */
  void resumeConnector(String connector);
}
