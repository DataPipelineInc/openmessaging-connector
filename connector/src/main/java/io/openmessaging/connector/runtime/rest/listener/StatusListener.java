package io.openmessaging.connector.runtime.rest.listener;

public interface StatusListener<T> {
  void onStartUp(T connectorOrTaskId);

  void onPause(T connectorOrTaskId);

  void onResume(T connectorOrTaskId);

  void onStop(T connectorOrTaskId);

  void onDeletion(T connectorOrTaskId);

  void onFailure(T connectorOrTaskId);
}
