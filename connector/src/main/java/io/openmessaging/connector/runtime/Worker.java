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
import io.openmessaging.connector.runtime.storage.SourcePositionCommitter;
import io.openmessaging.connector.runtime.utils.ConvertUtils;
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

/**
 * All of the Connector and Task are managed by this worker.
 * Each of them has its own dedicated thread. This Worker is their abstract container.
 */
public class Worker {
    private static final Logger log = LoggerFactory.getLogger(Worker.class);
    private MessagingAccessPoint messagingAccessPoint;
    private WorkerConfig workerConfig;
    private ExecutorService executorService;
    private Map<String, WorkerConnector> connectors;
    private Map<ConnectorTaskId, WorkerTask> tasks;
    private PositionStorageService positionStorageService;
    private SourcePositionCommitter committer;
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

    /**
     * Start the worker.
     */
    public void start() {
        log.info("Starting worker");
        this.positionStorageService.start();
        this.committer = new SourcePositionCommitter(workerConfig);
        log.info("Started worker");

    }

    /**
     * Stop the worker.
     */
    public void stop() {
        log.info("Stopping worker");
        if (!connectors.isEmpty()) {
            for (String connector : connectors.keySet()) {
                stopConnector(connector);
            }
        }
        if (!tasks.isEmpty()) {
            stopAndAwaitTasks(tasks.keySet());
        }
        this.committer.shutdowm(6000);
        this.positionStorageService.stop();
        log.info("Stopped worker");
    }


    /**
     * Start a connector managed by this worker.
     *
     * @param connectorName   the name of the connector.
     * @param connectorConfig the configuration of the connector.
     * @param targetState     the initial state of this connector.
     * @param statusListener  the listener whose connector state changes during the running process.
     * @return true if the connector is successfully started.
     */
    public boolean startConnector(
            String connectorName,
            Map<String, String> connectorConfig,
            TargetState targetState,
            StandaloneProcessor.ConnectorStatusListener statusListener) {
        Connector connector =
                plugins().newConnector(connectorConfig.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG));
        WorkerConnector workerConnector =
                new WorkerConnector(connector, connectorName, connectorConfig, statusListener);
        workerConnector.initialize();
        workerConnector.changeTargetState(targetState);
        connectors.put(connectorName, workerConnector);
        return true;
    }

    /**
     * Stop a connector managed by this worker by the name of the connector.
     *
     * @param connectorName the name of the connector to stop.
     * @return true if the connector is successfully stopped.
     */
    public boolean stopConnector(String connectorName) {
        if (!connectors.containsKey(connectorName)) {
            log.warn("Ignoring stop request for unowned connector {}", connectorName);
            return false;
        }
        WorkerConnector workerConnector = connectors.get(connectorName);
        workerConnector.stop();
        return true;
    }


    /**
     * Start a task managed by this worker.
     *
     * @param taskId         the id of the task.
     * @param taskConfig     the configuration of the task.
     * @param targetState    the initial state of the task.
     * @param statusListener the listener whose task state changes during the running process.
     * @return true if the task is successfully started.
     */
    public boolean startTask(
            ConnectorTaskId taskId,
            Map<String, String> taskConfig,
            TargetState targetState,
            StandaloneProcessor.TaskStatusListener statusListener) {
        WorkerTask workerTask = buildWorkerTask(taskId, taskConfig, targetState, statusListener);
        workerTask.initialize();
        tasks.put(taskId, workerTask);
        this.executorService.submit(workerTask);
        if (workerTask instanceof WorkerSourceTask) {
            committer.schedule(taskId, (WorkerSourceTask) workerTask);
        }
        return true;
    }

    /**
     * Create WorkerSourceTask or WorkerSinkTask by giving configuration information.
     *
     * @param taskId         the id of the task.
     * @param taskConfig     the configuration of the task.
     * @param targetState    the initial state of the task
     * @param statusListener the listener whose task state changes during the running process.
     * @return a WorkerSourceTask or WorkerSinkTask.
     */
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
            PositionStorageWriter positionStorageWriter =
                    new PositionStorageWriter(this.workerConfig, this.positionStorageService);
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

    /**
     * Get a list of configuration information of the task by calling the method of the connector.
     *
     * @param connectorName the name of the connector.
     * @return a list of configuration information of the task.
     */
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

    /**
     * Stop the given tasks asynchronously, then wait for these tasks to be terminated.
     *
     * @param taskIds the list of the taskId that we want to stop.
     */
    public void stopAndAwaitTasks(Collection<ConnectorTaskId> taskIds) {
        for (ConnectorTaskId taskId : taskIds) {
            stopAndAwaitTask(taskId);
        }
    }

    /**
     * Stop the given task asynchronously, then wait for the task to be terminated.
     *
     * @param taskId the taskId that we want to stop.
     */
    public void stopAndAwaitTask(ConnectorTaskId taskId) {
        stopTask(taskId);
        awaitTask(taskId);
    }

    /**
     * Stop giving the tasks.
     *
     * @param taskIds the collection of the taskId that we want to stop.
     */
    public void stopTasks(Collection<ConnectorTaskId> taskIds) {
        for (ConnectorTaskId taskId : taskIds) {
            stopTask(taskId);
        }
    }


    /**
     * Stop giving the task.
     *
     * @param taskId the taskId that we want to stop.
     */
    private void stopTask(ConnectorTaskId taskId) {
        WorkerTask workerTask = tasks.get(taskId);
        if (workerTask == null) {
            log.warn("Ignoring stop request for unowned task {}", taskId);
            return;
        }
        if (workerTask instanceof WorkerSourceTask) {
            committer.remove(taskId);
        }
        workerTask.stop();
    }


    /**
     * Wait for these tasks to be terminated.
     *
     * @param taskIds the collection of the taskId that we want to stop.
     */
    private void awaitTasks(Collection<ConnectorTaskId> taskIds) {
        for (ConnectorTaskId taskId : taskIds) {
            awaitTask(taskId);
        }
    }

    /**
     * Wait for the task to be terminated.
     *
     * @param taskId the taskId that we want to stop.
     */
    private void awaitTask(ConnectorTaskId taskId) {
        WorkerTask workerTask = tasks.get(taskId);
        if (workerTask == null) {
            log.warn("Ignoring await request for unowned task {}", taskId);
            return;
        }
        workerTask.awaitStop();
    }

    /**
     * Get the plugins of the worker.
     *
     * @return the plugins of the worker.
     */
    public Plugins plugins() {
        return plugins;
    }


    /**
     * Change the target state of this connector and the task belonging to this connector.
     *
     * @param targetState the target state.
     * @param connector   the name of the connector.
     */
    public void changeTargetState(TargetState targetState, String connector) {
        WorkerConnector workerConnector = connectors.get(connector);
        changeTargetState(workerConnector, targetState);
        for (Map.Entry<ConnectorTaskId, WorkerTask> entry : tasks.entrySet()) {
            if (entry.getKey().getConnectorName().equals(connector)) {
                changeTargetState(entry.getValue(), targetState);
            }
        }
    }

    /**
     * Change the target state through WorkerConnector or WorkerTask.
     *
     * @param workerConnectorOrTask WorkerConnector or WorkerTask.
     * @param targetState           the target state that we want to change.
     */

    private void changeTargetState(Object workerConnectorOrTask, TargetState targetState) {
        if (workerConnectorOrTask instanceof WorkerConnector) {
            ((WorkerConnector) workerConnectorOrTask).changeTargetState(targetState);
        } else if (workerConnectorOrTask instanceof WorkerTask) {
            ((WorkerTask) workerConnectorOrTask).changeTargetState(targetState);
        }
    }
}
