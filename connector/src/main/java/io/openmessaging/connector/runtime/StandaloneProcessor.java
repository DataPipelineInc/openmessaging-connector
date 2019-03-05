package io.openmessaging.connector.runtime;

import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.runtime.distributed.ClusterStateConfig;
import io.openmessaging.connector.runtime.isolation.Plugins;
import io.openmessaging.connector.runtime.rest.entities.ConnectorInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorStateInfo;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.entities.ConnectorType;
import io.openmessaging.connector.runtime.rest.entities.TaskInfo;
import io.openmessaging.connector.runtime.rest.error.ConnectException;
import io.openmessaging.connector.runtime.rest.listener.ConfigListener;
import io.openmessaging.connector.runtime.rest.listener.ConnectorStatusListener;
import io.openmessaging.connector.runtime.rest.listener.TaskStatusListener;
import io.openmessaging.connector.runtime.rest.storage.ConfigStorageService;
import io.openmessaging.connector.runtime.rest.storage.StatusStorageService;
import io.openmessaging.connector.runtime.utils.CallBack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StandaloneProcessor extends AbstractProcessor {
  private Map<String, Connector> tempConnector;
  private ConfigStorageService configStorageService;
  private StatusStorageService statusStorageService;
  private ConfigListener configListener;
  private ConnectorStatusListener connectorStatusListener;
  private TaskStatusListener taskStatusListener;
  private ClusterStateConfig stateConfig;
  private Worker worker;

  public StandaloneProcessor(
      ConfigStorageService configStorageService,
      StatusStorageService statusStorageService,
      Worker worker) {
    this.configStorageService = configStorageService;
    this.statusStorageService = statusStorageService;
    this.configListener = new ConfigChangeListener();
    this.connectorStatusListener = connectorStatusListener;
    this.taskStatusListener = taskStatusListener;
    this.stateConfig = ClusterStateConfig.EMPTY;
    this.worker = worker;
    this.tempConnector = new HashMap<>();
  }

  @Override
  public void start() {
    this.configStorageService.start();
    this.statusStorageService.start();
  }

  @Override
  public void stop() {
    this.configStorageService.stop();
    this.statusStorageService.stop();
  }

  @Override
  public List<String> connectors() {
    return new ArrayList<>(stateConfig.allConnector());
  }

  @Override
  public void putConnectorConfig(
      String connectorName, Map<String, String> config, CallBack<ConnectorInfo> callBack) {
    if (!validateConnectorConfig(config)) {
      callBack.onCompletion(
          new ConnectException(
              "The configuration of this connector did not pass the verification : " + config),
          null);
      return;
    }
    if (stateConfig.contains(connectorName)) {
      callBack.onCompletion(
          new ConnectException("Connector " + connectorName + " already exists"), null);
      return;
    }
    if (!startConnector(connectorName, config)) {
      callBack.onCompletion(
          new ConnectException("Failed to start the connector : " + connectorName), null);
    }
    createOrUpdateTaskConfig(connectorName);
    callBack.onCompletion(null, createConnectorInfo(connectorName));
  }

  private ConnectorInfo createConnectorInfo(String connector) {
    return new ConnectorInfo(
        connector,
        stateConfig.connectorConfig(connector),
        stateConfig.tasks(connector),
        getConnectorTypeFromClass(stateConfig.connectorConfig(connector).get("class")));
  }

  private ConnectorType getConnectorTypeFromClass(String className) {
    Connector connector = getConnector(className);
    return ConnectorType.fromClass(connector.getClass());
  }

  @Override
  public void connectorInfo(String connectorName, CallBack<ConnectorInfo> callBack) {
    if (stateConfig.contains(connectorName)) {
      callBack.onCompletion(null, createConnectorInfo(connectorName));
      return;
    } else {
      callBack.onCompletion(
          new ConnectException("Not found the connector :" + connectorName), null);
    }
  }

  private Connector getConnector(String className) {
    if (!tempConnector.containsKey(className)) {
      Connector connector = plugins().newConnector(className);
      tempConnector.put(className, connector);
      return connector;
    }
    return tempConnector.get(className);
  }

  private void createOrUpdateTaskConfig(String connector) {
    List<Map<String, String>> newConfig = newTaskConfig(connector);
    List<Map<String, String>> oldConfig = oldTaskConfig(connector);
    if (!oldConfig.equals(newConfig)) {
      removeConnectorTask(connector);
      configStorageService.putTaskConfig(connector, newConfig);
      createConnectorTask(connector);
    }
  }

  private List<Map<String, String>> newTaskConfig(String connector) {
    return worker.connectorTaskConfigs(connector);
  }

  private List<Map<String, String>> oldTaskConfig(String connector) {
    return stateConfig.allTaskConfigs(connector);
  }

  private void removeConnectorTask(String connector) {
    List<ConnectorTaskId> taskIds = stateConfig.tasks(connector);
    if (!taskIds.isEmpty()) {
      worker.stopAndAwaitTasks(taskIds);
      configStorageService.removeTaskConfig(connector);
    }
  }

  private void createConnectorTask(String connector) {
    List<ConnectorTaskId> taskIds = stateConfig.tasks(connector);
    for (ConnectorTaskId taskId : taskIds) {
      worker.startTask(taskId, stateConfig.taskConfig(taskId));
    }
  }

  private Plugins plugins() {
    return this.worker.plugins();
  }

  @Override
  public void putTaskConfig(
      String connectorName, List<Map<String, String>> configs, CallBack<List<TaskInfo>> callBack) {}

  public boolean startConnector(String connectorName, Map<String, String> config) {
    return false;
  }

  public boolean startTask(ConnectorTaskId taskId) {
    return false;
  }

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
    return true;
  }

  public class ConfigChangeListener implements ConfigListener {
    @Override
    public void onConnectorConfigUpdate(String connector) {}

    @Override
    public void onConnectorConfigDelete(String connector) {}

    @Override
    public void onTaskConfigUpdate(ConnectorTaskId taskId) {}

    @Override
    public void onConnectorTargerStateUpdate(String connector) {}
  }
}
