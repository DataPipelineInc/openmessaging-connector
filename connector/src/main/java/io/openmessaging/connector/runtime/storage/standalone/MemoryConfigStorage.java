package io.openmessaging.connector.runtime.storage.standalone;

import io.openmessaging.connector.runtime.TargetState;
import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.distributed.ClusterStateConfig;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.listener.ConfigListener;
import io.openmessaging.connector.runtime.rest.storage.ConfigStorageService;

import java.util.List;
import java.util.Map;

public class MemoryConfigStorage implements ConfigStorageService {
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
  public void putConnectorConfig(String connectorName, Map<String, String> connectorconfig) {

  }

  @Override
  public void putTaskConfig(String connector, List<Map<String, String>> taskConfig) {

  }

  @Override
  public void removeConnectorConfig(String connectorName) {

  }

  @Override
  public void removeTaskConfig(String connectorName) {

  }

  @Override
  public boolean contains(String connectorName) {
    return false;
  }

  @Override
  public void setConfigListener(ConfigListener listener) {

  }

  @Override
  public ClusterStateConfig snapshot() {
    return null;
  }

  @Override
  public void putTargetState(String connector, TargetState targetState) {

  }
}
