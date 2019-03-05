package io.openmessaging.connector.runtime.storage.standalone;

import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.rest.entities.ConnectorStatus;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.entities.TaskStatus;
import io.openmessaging.connector.runtime.rest.storage.StatusStorageService;

public class MemoryStatusStorage implements StatusStorageService {
  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  @Override
  public void initialize(WorkerConfig workerConfig) {

  }

  @Override
  public ConnectorStatus get(String connectorName) {
    return null;
  }

  @Override
  public TaskStatus get(ConnectorTaskId taskId) {
    return null;
  }

  @Override
  public void put(String connectorName, ConnectorStatus connectorStatus) {

  }

  @Override
  public void put(ConnectorTaskId taskId, TaskStatus taskStatus) {

  }
}
