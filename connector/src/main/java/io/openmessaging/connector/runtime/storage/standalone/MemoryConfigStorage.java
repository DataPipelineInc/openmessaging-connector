package io.openmessaging.connector.runtime.storage.standalone;

import io.openmessaging.connector.runtime.TargetState;
import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.distributed.ClusterStateConfig;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.listener.ConfigListener;
import io.openmessaging.connector.runtime.rest.storage.ConfigStorageService;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MemoryConfigStorage implements ConfigStorageService {
  private Map<String, ConnectorState> connectors;
  private ConfigListener configListener;

  public MemoryConfigStorage() {
    this.connectors = new HashMap<>();
  }

  private static Map<ConnectorTaskId, Map<String, String>> taskConfigListAsMap(
      String connector, List<Map<String, String>> configs) {
    int index = 0;
    Map<ConnectorTaskId, Map<String, String>> result = new TreeMap<>();
    for (Map<String, String> taskConfigMap : configs) {
      result.put(new ConnectorTaskId(connector, index++), taskConfigMap);
    }
    return result;
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void initialize(WorkerConfig workerConfig) {}

  @Override
  public void putConnectorConfig(String connectorName, Map<String, String> connectorconfig) {
    ConnectorState state = connectors.get(connectorName);
    if (state == null) connectors.put(connectorName, new ConnectorState(connectorconfig));
    else state.connConfig = connectorconfig;

    if (configListener != null) configListener.onConnectorConfigUpdate(connectorName);
  }

  @Override
  public void putTaskConfig(String connector, List<Map<String, String>> taskConfig) {
    ConnectorState state = connectors.get(connector);
    if (state == null)
      throw new IllegalArgumentException("Cannot put tasks for non-existing connector");

    Map<ConnectorTaskId, Map<String, String>> taskConfigsMap =
        taskConfigListAsMap(connector, taskConfig);
    state.taskConfigs = taskConfigsMap;

    if (configListener != null) configListener.onTaskConfigUpdate(taskConfigsMap.keySet());
  }

  @Override
  public void removeConnectorConfig(String connectorName) {
    ConnectorState state = connectors.remove(connectorName);

    if (configListener != null && state != null)
      configListener.onConnectorConfigDelete(connectorName);
  }

  @Override
  public void removeTaskConfig(String connectorName) {
    ConnectorState state = connectors.get(connectorName);
    if (state == null)
      throw new IllegalArgumentException("Cannot remove tasks for non-existing connector");

    HashSet<ConnectorTaskId> taskIds = new HashSet<>(state.taskConfigs.keySet());
    state.taskConfigs.clear();

    if (configListener != null) configListener.onTaskConfigUpdate(taskIds);
  }

  @Override
  public boolean contains(String connectorName) {
    return connectors.containsKey(connectorName);
  }

  @Override
  public void setConfigListener(ConfigListener configListener) {
    this.configListener = configListener;
  }

  @Override
  public ClusterStateConfig snapshot() {
    Map<String, Map<String, String>> connectorConfigs = new HashMap<>();
    Map<String, TargetState> connectorTargetStates = new HashMap<>();
    Map<ConnectorTaskId, Map<String, String>> taskConfigs = new HashMap<>();
    Map<String, Integer> connectorTaskCounts = new HashMap<>();

    for (Map.Entry<String, ConnectorState> connectorStateEntry : connectors.entrySet()) {
      String connector = connectorStateEntry.getKey();
      ConnectorState connectorState = connectorStateEntry.getValue();
      connectorConfigs.put(connector, connectorState.connConfig);
      connectorTargetStates.put(connector, connectorState.targetState);
      connectorTaskCounts.put(connector, connectorState.taskConfigs.size());
      taskConfigs.putAll(connectorState.taskConfigs);
    }
    return new ClusterStateConfig(
        connectorConfigs,
        taskConfigs,
        connectorTargetStates,
        new HashSet<>(connectorConfigs.keySet()),
        connectorTaskCounts);
  }

  @Override
  public void putTargetState(String connector, TargetState targetState) {
    ConnectorState connectorState = connectors.get(connector);
    if (connectorState == null)
      throw new IllegalArgumentException("No connector `" + connector + "` configured");

    connectorState.targetState = targetState;

    if (configListener != null) configListener.onConnectorTargerStateUpdate(connector);
  }

  private static class ConnectorState {
    private TargetState targetState;
    private Map<String, String> connConfig;
    private Map<ConnectorTaskId, Map<String, String>> taskConfigs;

    public ConnectorState(Map<String, String> connConfig) {
      this.targetState = TargetState.STARTED;
      this.connConfig = connConfig;
      this.taskConfigs = new HashMap<>();
    }
  }
}
