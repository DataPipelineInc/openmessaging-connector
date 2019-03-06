package io.openmessaging.connector.runtime.rest.entities;

public abstract class AbstractStatus<T> {
  public enum State {
    UNASSIGNED,
    RUNNING,
    PAUSED,
    FAILED,
    DESTROYED
  }

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
}
