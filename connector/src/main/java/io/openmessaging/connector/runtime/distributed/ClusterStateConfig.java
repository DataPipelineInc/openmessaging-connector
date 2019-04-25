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

/** This class holds an immutable snapshot of the current cluster config and state. */
public class ClusterStateConfig {
  public static ClusterStateConfig EMPTY =
      new ClusterStateConfig(
          Collections.EMPTY_MAP,
          Collections.EMPTY_MAP,
          Collections.EMPTY_MAP,
          Collections.EMPTY_SET,
          Collections.EMPTY_MAP);
  private Map<String, Map<String, String>> connectors;
  private Map<ConnectorTaskId, Map<String, String>> tasks;
  private Map<String, TargetState> targetStates;
  private Set<String> allConnector;
  private Map<String, Integer> connectorTaskCounts;

  public ClusterStateConfig(
      Map<String, Map<String, String>> connectors,
      Map<ConnectorTaskId, Map<String, String>> tasks,
      Map<String, TargetState> targetStates,
      Set<String> allConnector,
      Map<String, Integer> connectorTaskCounts) {
    this.connectors = connectors;
    this.tasks = tasks;
    this.targetStates = targetStates;
    this.allConnector = allConnector;
    this.connectorTaskCounts = connectorTaskCounts;
  }

  /**
   * Get the configuration of the given connector.
   *
   * @param connector the name of the connector.
   * @return the configuration of the given connector.
   */
  public Map<String, String> connectorConfig(String connector) {
    return connectors.get(connector);
  }

  /**
   * Get the configuration of the given task.
   *
   * @param taskId the id of the task.
   * @return the configuration of the given task.
   */
  public Map<String, String> taskConfig(ConnectorTaskId taskId) {
    return tasks.get(taskId);
  }

  /**
   * Get the target state of the given connector.
   *
   * @param connector the id of the connector.
   * @return the target state of the given connector.
   */
  public TargetState targetState(String connector) {
    return targetStates.get(connector);
  }

  /**
   * Get the names of all connectors.
   *
   * @return a set of names for all connectors.
   */
  public Set<String> allConnector() {
    return allConnector;
  }

  /**
   * Get all the task configuration information of the connector in the current snapshot.
   *
   * @param connector the name of the connector.
   * @return a list of the task configuration in the current snapshot.
   */
  public List<Map<String, String>> allTaskConfigs(String connector) {
    Map<ConnectorTaskId, Map<String, String>> allTaskConfigs = new TreeMap<>();
    for (Map.Entry<ConnectorTaskId, Map<String, String>> entry : tasks.entrySet()) {
      if (entry.getKey().getConnectorName().equals(connector)) {
        allTaskConfigs.put(entry.getKey(), entry.getValue());
      }
    }
    return new LinkedList<>(allTaskConfigs.values());
  }

  /**
   * Get a list of the taskId of the given connector.
   *
   * @param connector the name of the connector.
   * @return a list of the taskId.
   */
  public List<ConnectorTaskId> tasks(String connector) {
    List<ConnectorTaskId> tasks = new ArrayList<>();
    int taskNum = connectorTaskCounts.get(connector);
    for (int i = 0; i < taskNum; i++) {
      tasks.add(new ConnectorTaskId(connector, i));
    }
    return tasks;
  }

  /**
   * Check if the configuration of this connector is included in this snapshot.
   *
   * @param connector the name of the connector.
   * @return true if this snapshot contains the configuration of this connector ,false otherwise.
   */
  public boolean contains(String connector) {
    return connectors.containsKey(connector);
  }
}
