package io.openmessaging.connector.runtime.rest.entities;

import java.util.List;

public class ConnectorStateInfo {
  private String connectorName;
  private ConnectorState connectorState;
  private List<TaskState> taskStates;
  private ConnectorType connectorType;

  public ConnectorStateInfo(
      String connectorName,
      ConnectorState connectorState,
      List<TaskState> taskStates,
      ConnectorType connectorType) {
    this.connectorName = connectorName;
    this.connectorState = connectorState;
    this.taskStates = taskStates;
    this.connectorType = connectorType;
  }

  public String getConnectorName() {
    return connectorName;
  }

  public ConnectorState getConnectorState() {
    return connectorState;
  }

  public List<TaskState> getTaskStates() {
    return taskStates;
  }

  public ConnectorType getConnectorType() {
    return connectorType;
  }

  public static class ConnectorState extends AbstractState {
    private String connectorName;

    public ConnectorState(String connectorName, String state) {
      super(state);
      this.connectorName = connectorName;
    }

    public String getConnectorName() {
      return connectorName;
    }
  }

  public static class TaskState extends AbstractState {
    private ConnectorTaskId taskId;

    public TaskState(ConnectorTaskId taskId, String state) {
      super(state);
      this.taskId = taskId;
    }

    public ConnectorTaskId getTaskId() {
      return taskId;
    }
  }

  public abstract static class AbstractState {
    private String state;

    public AbstractState(String state) {
      this.state = state;
    }

    public String getState() {
      return state;
    }
  }
}
