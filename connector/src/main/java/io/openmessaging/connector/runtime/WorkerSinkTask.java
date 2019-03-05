package io.openmessaging.connector.runtime;

import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.listener.TaskStatusListener;

public class WorkerSinkTask extends WorkerTask {
  public WorkerSinkTask(ConnectorTaskId taskId, TargetState targetState, TaskStatusListener listener) {
    super(taskId, targetState, listener);
  }

  @Override
  public void execute() {

  }
}
