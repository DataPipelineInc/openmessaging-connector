package io.openmessaging.connector.runtime.rest.entities;

public class TaskStatus extends AbstractStatus<ConnectorTaskId> {
  public TaskStatus(ConnectorTaskId id, State state) {
    super(id, state);
  }
}
