package io.openmessaging.connector.runtime.rest.entities;

/**
 * Some states common to the connector and task.
 *
 * @param <T> ConnectorTaskId or String.
 */
public abstract class AbstractStatus<T> {
  private T id;
  private State state;

  public AbstractStatus(T id, State state) {
    this.id = id;
    this.state = state;
  }

  public T getId() {
    return id;
  }

  public State getState() {
    return state;
  }

  public enum State {
    UNASSIGNED,
    RUNNING,
    PAUSED,
    FAILED,
    DESTROYED
  }
}
