package io.openmessaging.connector.runtime.rest.listener;

import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;

public interface ConfigListener {
  void onConnectorConfigUpdate(String connector);
  void onConnectorConfigDelete(String connector);
  void onTaskConfigUpdate(ConnectorTaskId taskId);

}
