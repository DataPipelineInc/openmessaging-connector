package io.openmessaging.connector.runtime.rest.listener;

import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;

public class TaskStatusListener implements StatusListener<ConnectorTaskId> {
  @Override
  public void onStartUp(ConnectorTaskId connectorOrTaskId) {

  }

  @Override
  public void onPause(ConnectorTaskId connectorOrTaskId) {

  }

  @Override
  public void onResume(ConnectorTaskId connectorOrTaskId) {

  }

  @Override
  public void onStop(ConnectorTaskId connectorOrTaskId) {

  }

  @Override
  public void onDeletion(ConnectorTaskId connectorOrTaskId) {

  }

  @Override
  public void onFailure(ConnectorTaskId connectorOrTaskId,Throwable throwable) {

  }
}
