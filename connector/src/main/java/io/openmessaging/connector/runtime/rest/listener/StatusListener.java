package io.openmessaging.connector.runtime.rest.listener;

public interface StatusListener<T> {

  /**
   * Invoked after the connector or task has been started.
   *
   * @param connectorOrTaskId the name of connector or the taskId of task
   */
  void onStartUp(T connectorOrTaskId);

  /**
   * Invoked after the connector or task has been paused.
   *
   * @param connectorOrTaskId the name of connector or the taskId of task.
   */
  void onPause(T connectorOrTaskId);

  /**
   * Invoked after the connector or task has been resumed.
   *
   * @param connectorOrTaskId the name of connector or the taskId of task.
   */
  void onResume(T connectorOrTaskId);

  /**
   * Invoked after the connector or task has been stopped.
   *
   * @param connectorOrTaskId the name of connector or the taskId of task.
   */
  void onShutDown(T connectorOrTaskId);

  /**
   * Invoked after the connector or task has been deleted.
   *
   * @param connectorOrTaskId the name of connector or the taskId of task.
   */
  void onDeletion(T connectorOrTaskId);

  /**
   * Invoked after the connector or task has throwed an exception.
   *
   * @param connectorOrTaskId the name of connector or the taskId of task.
   * @param throwable error throwed from connector or task.
   */
  void onFailure(T connectorOrTaskId, Throwable throwable);
}
