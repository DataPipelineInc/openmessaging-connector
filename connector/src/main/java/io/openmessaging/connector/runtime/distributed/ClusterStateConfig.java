package io.openmessaging.connector.runtime.distributed;

import io.openmessaging.connector.runtime.TargetState;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ClusterStateConfig {
  private Map<String, Map<String, String>> connectors;
  private Map<ConnectorTaskId, Map<String, String>> tasks;
  private Map<String, TargetState> targetStates;
  private Set<String> allConnector;
  public static ClusterStateConfig EMPTY =
      new ClusterStateConfig(
          Collections.EMPTY_MAP,
          Collections.EMPTY_MAP,
          Collections.EMPTY_MAP,
          Collections.EMPTY_SET);

  public ClusterStateConfig(
      Map<String, Map<String, String>> connectors,
      Map<ConnectorTaskId, Map<String, String>> tasks,
      Map<String, TargetState> targetStates,
      Set<String> allConnector) {
    this.connectors = connectors;
    this.tasks = tasks;
    this.targetStates = targetStates;
    this.allConnector = allConnector;
  }

  public Map<String, String> connectorConfig(String connector) {
    return connectors.get(connector);
  }

  public Map<String, String> taskConfig(ConnectorTaskId taskId) {
    return tasks.get(taskId);
  }

  public TargetState targetState(String connector) {
    return targetStates.get(connector);
  }

  public Set<String> allConnector() {
    return allConnector;
  }

  public List<Map<String, String>> allTaskConfigs(String connector) {
    Map<ConnectorTaskId, Map<String, String>> allTaskConfigs = new TreeMap<>();
    for (Map.Entry<ConnectorTaskId, Map<String, String>> entry : tasks.entrySet()) {
      if (entry.getKey().getConnectorName().equals(connector)) {
        allTaskConfigs.put(entry.getKey(), entry.getValue());
      }
    }
    return new LinkedList<>(allTaskConfigs.values());
  }

  public List<ConnectorTaskId> tasks(String connector) {
    List<ConnectorTaskId> tasks = new ArrayList<>();
    Map<String, String> connectorConfig = connectors.get(connector);
    int taskNum = Integer.parseInt(connectorConfig.get("tasks.max"));
    for (int i = 0; i < taskNum; i++) {
      tasks.add(new ConnectorTaskId(connector, i));
    }
    return tasks;
  }

  public boolean contains(String connector) {
    return connectors.containsKey(connector);
  }
}
