package io.openmessaging.connector.runtime;

import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.runtime.rest.entities.ConnectorInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorStateInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.entities.TaskInfo;
import io.openmessaging.connector.runtime.utils.CallBack;

import java.util.List;
import java.util.Map;

/** The main processor of the Connector and Task */
public interface Processor {
  void start();

  void stop();

  List<String> connectors();

  void putConnectorConfig(
      String connectorName, Map<String, String> config, CallBack<ConnectorInfo> callBack);

  void putTaskConfig(
      String connectorName, List<Map<String, String>> configs, CallBack<List<TaskInfo>> callBack);

  void deleteConnectorConfig(String connector);

  void connectorConfig(String connector, CallBack<Map<String,String>> callBack);

  void taskConfigs(String connector, CallBack<List<Map<String,String>>> callBack);

  void connectorStatus(String connector, CallBack<ConnectorStateInfo> callBack);

  void taskStatus(ConnectorTaskId taskId, CallBack<ConnectorStateInfo.TaskState> callBack);

  void restartConnector(String connector);

  void restartTask(ConnectorTaskId taskId);

  void pauseConnector(String connector);

  void resumeConnector(String connector);

  boolean validateConnectorConfig(Map<String, String> configs);
}
