package io.openmessaging.connector.runtime;

import io.openmessaging.connector.runtime.AbstractProcessor;
import io.openmessaging.connector.runtime.Worker;
import io.openmessaging.connector.runtime.rest.entities.ConnectorInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorStateInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.entities.TaskInfo;
import io.openmessaging.connector.runtime.rest.listener.ConfigListener;
import io.openmessaging.connector.runtime.rest.listener.ConnectorStatusListener;
import io.openmessaging.connector.runtime.rest.listener.TaskStatusListener;
import io.openmessaging.connector.runtime.rest.storage.ConfigStorageService;
import io.openmessaging.connector.runtime.rest.storage.StatusStorageService;

import java.util.List;
import java.util.Map;

public class StandaloneProcessor extends AbstractProcessor {
  private ConfigStorageService configStorageService;
  private StatusStorageService statusStorageService;
  private ConfigListener configListener;
  private ConnectorStatusListener connectorStatusListener;
  private TaskStatusListener taskStatusListener;
  private Worker worker;

  public StandaloneProcessor() {}

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public List<String> connectors() {
    return null;
  }

  @Override
  public ConnectorInfo putConnectorConfig(
      String connectorName, Map<String, String> connectorConfig) {
    return null;
  }

  @Override
  public List<TaskInfo> putTaskConfig(String connectorName, List<Map<String, String>> taskConfigs) {
    return null;
  }

  @Override
  public boolean startConnector(String connectorName) {
    return false;
  }

  @Override
  public boolean startTask(ConnectorTaskId taskId) {
    return false;
  }

  @Override
  public void deleteConnectorConfig(String connector) {}

  @Override
  public ConnectorInfo connectorConfig(String connector) {
    return null;
  }

  @Override
  public List<TaskInfo> taskConfigs(String connector) {
    return null;
  }

  @Override
  public ConnectorStateInfo connectorStatus(String connector) {
    return null;
  }

  @Override
  public ConnectorStateInfo.TaskState taskStatus(ConnectorTaskId taskId) {
    return null;
  }

  @Override
  public void restartConnector(String connector) {}

  @Override
  public void restartTask(ConnectorTaskId taskId) {}

  @Override
  public void pauseConnector(String connector) {}

  @Override
  public void resumeConnector(String connector) {}

  @Override
  public void validateConnectorConfig(Map<String, String> configs) {}
}
