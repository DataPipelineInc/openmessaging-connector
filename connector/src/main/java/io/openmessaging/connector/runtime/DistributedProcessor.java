package io.openmessaging.connector.runtime;

import io.openmessaging.connector.runtime.rest.entities.ConnectorInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorStateInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.entities.TaskInfo;
import io.openmessaging.connector.runtime.utils.CallBack;

import java.util.List;
import java.util.Map;

public class DistributedProcessor extends AbstractProcessor {

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public List<String> connectors() {
    return null;
  }

  @Override
  public void putConnectorConfig(
      String connectorName, Map<String, String> config, CallBack<ConnectorInfo> callBack) {}

  @Override
  public void putTaskConfig(
      String connectorName, List<Map<String, String>> configs, CallBack<List<TaskInfo>> callBack) {}

  @Override
  public void connectorInfo(String connectorName, CallBack<ConnectorInfo> callBack) {}

  @Override
  public void deleteConnectorConfig(String connector) {}

  @Override
  public void connectorConfig(String connector, CallBack<Map<String, String>> callBack) {}

  @Override
  public void taskConfigs(String connector, CallBack<List<TaskInfo>> callBack) {}

  @Override
  public void connectorStatus(String connector, CallBack<ConnectorStateInfo> callBack) {}

  @Override
  public void taskStatus(ConnectorTaskId taskId, CallBack<ConnectorStateInfo.TaskState> callBack) {}

  @Override
  public void restartConnector(String connector) {}

  @Override
  public void restartTask(ConnectorTaskId taskId) {}

  @Override
  public void pauseConnector(String connector) {}

  @Override
  public void resumeConnector(String connector) {}

  @Override
  public boolean validateConnectorConfig(Map<String, String> configs) {
    return false;
  }
}
