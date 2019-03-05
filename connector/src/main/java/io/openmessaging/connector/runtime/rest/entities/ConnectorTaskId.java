package io.openmessaging.connector.runtime.rest.entities;

public class ConnectorTaskId implements Comparable<ConnectorTaskId> {
  private String connectorName;
  private int taskId;

  public ConnectorTaskId(String connectorName, int taskId) {
    this.connectorName = connectorName;
    this.taskId = taskId;
  }

  public String getConnectorName() {
    return connectorName;
  }

  public int getTaskId() {
    return taskId;
  }

  @Override
  public int compareTo(ConnectorTaskId o) {
    int connectorCmp = connectorName.compareTo(o.connectorName);
    if (connectorCmp != 0) return connectorCmp;
    return Integer.compare(taskId, o.taskId);
  }
}
