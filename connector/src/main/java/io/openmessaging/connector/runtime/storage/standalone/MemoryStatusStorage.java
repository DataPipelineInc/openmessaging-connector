package io.openmessaging.connector.runtime.storage.standalone;

import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.rest.entities.ConnectorStatus;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.entities.TaskStatus;
import io.openmessaging.connector.runtime.rest.storage.StatusStorageService;
import java.util.HashMap;
import java.util.Map;

public class MemoryStatusStorage implements StatusStorageService {
  private Map<String, ConnectorStatus> connectors;
  private Map<ConnectorTaskId, TaskStatus> tasks;

  public MemoryStatusStorage() {
    this.connectors = new HashMap<>();
    this.tasks = new HashMap<>();
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void initialize(WorkerConfig workerConfig) {}

  @Override
  public ConnectorStatus get(String connectorName) {
    return this.connectors.get(connectorName);
  }

  @Override
  public TaskStatus get(ConnectorTaskId taskId) {
    return this.tasks.get(taskId);
  }

  @Override
  public void put(String connectorName, ConnectorStatus connectorStatus) {
    this.connectors.put(connectorName, connectorStatus);
  }

  @Override
  public void put(ConnectorTaskId taskId, TaskStatus taskStatus) {
    this.tasks.put(taskId, taskStatus);
  }
}
