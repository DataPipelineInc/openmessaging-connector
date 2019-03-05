package io.openmessaging.connector.runtime;

import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;

public class WorkerSinkTask extends WorkerTask {
  public WorkerSinkTask(ConnectorTaskId taskId, TargetState targetState, StandaloneProcessor.TaskStatusListener listener) {
    super(taskId, targetState, listener);
  }

  @Override
  public void execute() {

  }
}
