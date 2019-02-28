package io.openmessaging.connector.runtime;

import io.openmessaging.connector.api.Connector;

public class WorkerConnector {
  private Connector connector;

  public WorkerConnector(Connector connector) {
    this.connector = connector;
  }

  public void initialize() {}

  public void start() {}

  public void stop() {}

  public Connector getConnector() {
    return connector;
  }
}
