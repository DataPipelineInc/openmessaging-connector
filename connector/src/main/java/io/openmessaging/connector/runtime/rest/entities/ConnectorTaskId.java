package io.openmessaging.connector.runtime.rest.entities;

public class ConnectorTaskId {
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
}
