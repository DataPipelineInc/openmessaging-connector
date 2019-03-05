package io.openmessaging.connector.runtime;

import io.openmessaging.KeyValue;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.api.PositionStorageReader;
import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.connector.runtime.isolation.Plugins;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.error.ConnectException;
import io.openmessaging.connector.runtime.rest.storage.PositionStorageService;
import io.openmessaging.connector.runtime.storage.PositionStorageWriter;
import io.openmessaging.connector.runtime.utils.ConvertUtils;
import io.openmessaging.internal.MessagingAccessPointAdapter;
import io.openmessaging.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Worker {
  private static final Logger log = LoggerFactory.getLogger(Worker.class);
  private MessagingAccessPoint messagingAccessPoint;
  private WorkerConfig workerConfig;
  private ExecutorService executorService;
  private Map<String, WorkerConnector> connectors;
  private Map<ConnectorTaskId, WorkerTask> tasks;
  private PositionStorageService positionStorageService;
  private Plugins plugins;

  public Worker(
      WorkerConfig workerConfig, PositionStorageService positionStorageService, Plugins plugins) {
    this.executorService = Executors.newCachedThreadPool();
    this.connectors = new ConcurrentHashMap<>();
    this.tasks = new ConcurrentHashMap<>();
    this.workerConfig = workerConfig;
    this.positionStorageService = positionStorageService;
    this.plugins = plugins;
    this.messagingAccessPoint =
        OMS.getMessagingAccessPoint(
            workerConfig.getMessagingSystemConfig().getString("accessPoint"), OMS.newKeyValue());
  }

  public boolean startConnector(
      String connectorName,
      Map<String, String> connectorConfig,
      TargetState targetState,
      StandaloneProcessor.ConnectorStatusListener statusListener) {
    Connector connector =
        Plugins.newConnector(connectorConfig.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG));
    WorkerConnector workerConnector =
        new WorkerConnector(connector, connectorName, connectorConfig, statusListener);
    workerConnector.initialize();
    workerConnector.changeTargetState(targetState);
    connectors.put(connectorName, workerConnector);
    return true;
  }

  public boolean startTask(
      ConnectorTaskId taskId,
      Map<String, String> taskConfig,
      TargetState targetState,
      StandaloneProcessor.TaskStatusListener statusListener) {
    WorkerTask workerTask = buildWorkerTask(taskId, taskConfig, targetState, statusListener);
    workerTask.initialize();
    tasks.put(taskId, workerTask);
    this.executorService.submit(workerTask);
    return true;
  }

  private WorkerTask buildWorkerTask(
      ConnectorTaskId taskId,
      Map<String, String> taskConfig,
      TargetState targetState,
      StandaloneProcessor.TaskStatusListener statusListener) {
    String taskClass = taskConfig.get(TaskConfig.TASK_CLASS_CONFIG);
    Task task = plugins.newTask(taskClass);
    if (task instanceof SourceTask) {
      Producer producer = messagingAccessPoint.createProducer();
      PositionStorageReader positionStorageReader =
          new io.openmessaging.connector.runtime.storage.PositionStorageReader();
      PositionStorageWriter positionStorageWriter = new PositionStorageWriter();
      return new WorkerSourceTask(
          taskId,
          (SourceTask) task,
          targetState,
          statusListener,
          workerConfig,
          positionStorageReader,
          positionStorageWriter,
          taskConfig,
          producer);
    }
    return null;
  }

  public List<Map<String, String>> connectorTaskConfigs(String connectorName) {
    WorkerConnector workerConnector = connectors.get(connectorName);
    if (workerConnector == null) {
      throw new ConnectException("Connector " + connectorName + " not found in this worker.");
    }
    Connector connector = workerConnector.getConnector();
    List<Map<String, String>> configs = new ArrayList<>();
    for (KeyValue config : connector.taskConfigs()) {
      configs.add(ConvertUtils.keyValueToMap(config));
    }
    return configs;
  }

  public void stopAndAwaitTasks(Collection<ConnectorTaskId> taskIds) {
    for (ConnectorTaskId taskId : taskIds) {
      stopAndAwaitTask(taskId);
    }
  }

  public void stopAndAwaitTask(ConnectorTaskId taskId) {}

  public void stopTasks(Collection<ConnectorTaskId> taskIds) {
    for (ConnectorTaskId taskId : taskIds) {
      stopTask(taskId);
    }
  }

  public void stopTask(ConnectorTaskId taskId) {
    WorkerTask workerTask = tasks.get(taskId);
    workerTask.stop();
  }

  private void awaitTasks(Collection<ConnectorTaskId> taskIds) {
    for (ConnectorTaskId taskId : taskIds) {
      awaitTask(taskId);
    }
  }

  private void awaitTask(ConnectorTaskId taskId) {}

  public Plugins plugins() {
    return plugins;
  }

  public void changeTargetState(TargetState targetState, String connector) {
    WorkerConnector workerConnector = connectors.get(connector);
    changeTargetState(workerConnector, targetState);
    for (Map.Entry<ConnectorTaskId, WorkerTask> entry : tasks.entrySet()) {
      if (entry.getKey().getConnectorName().equals(connector)) {
        changeTargetState(entry.getValue(), targetState);
      }
    }
  }

  private void changeTargetState(Object workerConnectorOrTask, TargetState targetState) {
    if (workerConnectorOrTask instanceof WorkerConnector) {
      ((WorkerConnector) workerConnectorOrTask).changeTargetState(targetState);
    } else if (workerConnectorOrTask instanceof WorkerTask) {
      ((WorkerTask) workerConnectorOrTask).changeTargerState(targetState);
    }
  }
}
