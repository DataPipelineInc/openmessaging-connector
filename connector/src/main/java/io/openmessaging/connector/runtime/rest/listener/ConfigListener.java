package io.openmessaging.connector.runtime.rest.listener;

import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;

public interface ConfigListener {

  /**
   * Invoked when a connector`s configuration has been created or updated.
   *
   * @param connector the connector`s name.
   */
  void onConnectorConfigUpdate(String connector);
  /**
   * Invoked when a connector`s configuration has been deleted.
   *
   * @param connector the connector`s name.
   */
  void onConnectorConfigDelete(String connector);
  /**
   * Invoked when a tast`s configuration has been created or updated.
   *
   * @param taskId the task`s is.
   */
  void onTaskConfigUpdate(ConnectorTaskId taskId);
}
