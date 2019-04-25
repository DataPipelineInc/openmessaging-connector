package io.openmessaging.connector.runtime.rest.entities;

import java.util.Map;

/** Contains the unique ID of the Task and the configutation of this task. */
public class TaskInfo {
  private ConnectorTaskId taskId;
  private Map<String, String> taskConfig;

  public TaskInfo(ConnectorTaskId taskId, Map<String, String> taskConfig) {
    this.taskId = taskId;
    this.taskConfig = taskConfig;
  }

  public ConnectorTaskId getTaskId() {
    return taskId;
  }

  public Map<String, String> getTaskConfig() {
    return taskConfig;
  }
}
