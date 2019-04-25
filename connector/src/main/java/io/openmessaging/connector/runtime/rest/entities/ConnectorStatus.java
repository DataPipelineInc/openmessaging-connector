package io.openmessaging.connector.runtime.rest.entities;

public class ConnectorStatus extends AbstractStatus<String> {
  public ConnectorStatus(String id, State state) {
    super(id, state);
  }
}
