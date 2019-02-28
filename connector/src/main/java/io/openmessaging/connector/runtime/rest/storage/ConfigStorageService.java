package io.openmessaging.connector.runtime.rest.storage;

import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.listener.ConfigListener;

import java.util.Map;

public interface ConfigStorageService {
  /** Start the position storage service. */
  void start();
  /** Stop the position storage service. */
  void stop();

  void initialize(WorkerConfig workerConfig);

  void putConnectorConfig(String connectorName, Map<String, String> connectorconfig);

  void putTaskConfig(ConnectorTaskId taskId, Map<String, String> taskConfig);

  void removeConnectorConfig(String connectorName);

  void removeTaskConfig(ConnectorTaskId taskId);

  void contains(String connectorName);

  void setConfigListener(ConfigListener listener);
}
