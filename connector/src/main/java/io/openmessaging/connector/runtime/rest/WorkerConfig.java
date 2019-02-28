package io.openmessaging.connector.runtime.rest;

public class WorkerConfig {
  private String hostname;
  private int port;

  public WorkerConfig(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
  }

  public String hostname() {
    return hostname;
  }

  public int port() {
    return port;
  }
}
