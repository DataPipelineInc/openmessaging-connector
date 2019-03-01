package io.openmessaging.connector.runtime;

import io.openmessaging.connector.runtime.rest.entities.ConnectorInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorStateInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.entities.TaskInfo;

import java.util.List;
import java.util.Map;

/** The main processor of the Connector and Task */
public interface Processor {
  void start();

  void stop();

  List<String> connectors();

  ConnectorInfo putConnectorConfig(String connectorName, Map<String, String> connectorConfig);

  List<TaskInfo> putTaskConfig(String connectorName, List<Map<String, String>> taskConfigs);

  boolean startConnector(String connectorName);

  boolean startTask(ConnectorTaskId taskId);

  void deleteConnectorConfig(String connector);

  ConnectorInfo connectorConfig(String connector);

  List<TaskInfo> taskConfigs(String connector);

  ConnectorStateInfo connectorStatus(String connector);

  ConnectorStateInfo.TaskState taskStatus(ConnectorTaskId taskId);

  void restartConnector(String connector);

  void restartTask(ConnectorTaskId taskId);

  void pauseConnector(String connector);

  void resumeConnector(String connector);

  void validateConnectorConfig(Map<String, String> configs);
}
