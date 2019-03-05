package io.openmessaging.connector.runtime;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.runtime.isolation.Plugins;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.error.ConnectException;
import io.openmessaging.connector.runtime.rest.storage.PositionStorageService;
import io.openmessaging.connector.runtime.utils.ConvertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Worker {
  private static final Logger log = LoggerFactory.getLogger(Worker.class);
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
  }

  public boolean startConnector(String connectorName, Map<String, String> connectorConfig) {
    String className = connectorConfig.get("connector.class");
    try {
      Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
      Connector connector =
          ((Class<? extends Connector>) clazz).getDeclaredConstructor().newInstance();
      WorkerConnector workerConnector = new WorkerConnector(connector);
      workerConnector.initialize();
      workerConnector.start();
      connectors.put(connectorName, workerConnector);
      return true;
    } catch (ClassNotFoundException e) {
      throw new ConnectException("Class not found : " + className);
    } catch (IllegalAccessException
        | InvocationTargetException
        | InstantiationException
        | NoSuchMethodException e) {
      throw new ConnectException("Can not instance the connector : " + className);
    }
  }

  public boolean startTask(ConnectorTaskId taskId, Map<String, String> taskConfig) {

    return true;
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
    for()
  }

  public void stopAndAwaitTask(ConnectorTaskId taskId) {}

  public void stopTasks(Collection<ConnectorTaskId> taskIds) {}

  public void stopTask(ConnectorTaskId taskId) {}

  private void awaitTasks() {}

  private void awaitTask() {}

  public Plugins plugins() {
    return plugins;
  }
}
