package io.openmessaging.connector.runtime;

import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.runtime.distributed.ClusterStateConfig;
import io.openmessaging.connector.runtime.isolation.Plugins;
import io.openmessaging.connector.runtime.rest.entities.*;
import io.openmessaging.connector.runtime.rest.error.ConnectException;
import io.openmessaging.connector.runtime.rest.listener.ConfigListener;
import io.openmessaging.connector.runtime.rest.listener.StatusListener;
import io.openmessaging.connector.runtime.rest.storage.ConfigStorageService;
import io.openmessaging.connector.runtime.rest.storage.StatusStorageService;
import io.openmessaging.connector.runtime.utils.CallBack;

import java.util.*;

public class StandaloneProcessor extends AbstractProcessor {
    private Map<String, Connector> tempConnector;
    private ConfigStorageService configStorageService;
    private StatusStorageService statusStorageService;
    private ConfigListener configListener;
    private ConnectorStatusListener connectorStatusListener;
    private TaskStatusListener taskStatusListener;
    private ClusterStateConfig stateConfig;
    private Worker worker;

    public StandaloneProcessor(
            ConfigStorageService configStorageService,
            StatusStorageService statusStorageService,
            Worker worker) {
        this.configStorageService = configStorageService;
        this.statusStorageService = statusStorageService;
        this.configListener = new ConfigChangeListener();
        this.connectorStatusListener = new ConnectorStatusListener();
        this.taskStatusListener = new TaskStatusListener();
        this.stateConfig = ClusterStateConfig.EMPTY;
        this.worker = worker;
        this.tempConnector = new HashMap<>();
        this.configStorageService.setConfigListener(this.configListener);
    }

    @Override
    public void start() {
        this.configStorageService.start();
        this.statusStorageService.start();
    }

    @Override
    public void stop() {
        this.configStorageService.stop();
        this.statusStorageService.stop();
    }

    @Override
    public List<String> connectors() {
        return new ArrayList<>(stateConfig.allConnector());
    }

    @Override
    public void putConnectorConfig(
            String connectorName, Map<String, String> config, CallBack<ConnectorInfo> callBack) {
        if (!validateConnectorConfig(config)) {
            callBack.onCompletion(
                    new ConnectException(
                            "The configuration of this connector did not pass the verification : " + config),
                    null);
            return;
        }
        if (stateConfig.contains(connectorName)) {
            callBack.onCompletion(
                    new ConnectException("Connector " + connectorName + " already exists"), null);
            return;
        }
        if (!startConnector(connectorName, config)) {
            callBack.onCompletion(
                    new ConnectException("Failed to start the connector : " + connectorName), null);
            return;
        }
        createOrUpdateTaskConfig(connectorName);
        callBack.onCompletion(null, createConnectorInfo(connectorName));
    }

    private ConnectorInfo createConnectorInfo(String connector) {
        return new ConnectorInfo(
                connector,
                stateConfig.connectorConfig(connector),
                stateConfig.tasks(connector),
                getConnectorTypeFromClass(
                        stateConfig.connectorConfig(connector).get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)));
    }

    private ConnectorType getConnectorTypeFromClass(String className) {
        Connector connector = getConnector(className);
        return ConnectorType.fromClass(connector.getClass());
    }

    @Override
    public void connectorInfo(String connectorName, CallBack<ConnectorInfo> callBack) {
        if (!stateConfig.contains(connectorName)) {
            callBack.onCompletion(
                    new ConnectException("The connector does not exist ; " + connectorName), null);
            return;
        }
        callBack.onCompletion(null, createConnectorInfo(connectorName));
    }

    private Connector getConnector(String className) {
        if (!tempConnector.containsKey(className)) {
            Connector connector = plugins().newConnector(className);
            tempConnector.put(className, connector);
            return connector;
        }
        return tempConnector.get(className);
    }

    private void createOrUpdateTaskConfig(String connector) {
        List<Map<String, String>> newConfig = newTaskConfig(connector);
        List<Map<String, String>> oldConfig = oldTaskConfig(connector);
        if (!oldConfig.equals(newConfig)) {
            removeConnectorTask(connector);
            configStorageService.putTaskConfig(connector, newConfig);
            createConnectorTask(connector, stateConfig.targetState(connector));
        }
    }

    private List<Map<String, String>> newTaskConfig(String connector) {
        return worker.connectorTaskConfigs(connector);
    }

    private List<Map<String, String>> oldTaskConfig(String connector) {
        return stateConfig.allTaskConfigs(connector);
    }

    private void removeConnectorTask(String connector) {
        List<ConnectorTaskId> taskIds = stateConfig.tasks(connector);
        if (!taskIds.isEmpty()) {
            worker.stopAndAwaitTasks(taskIds);
            configStorageService.removeTaskConfig(connector);
        }
    }

    private void createConnectorTask(String connector, TargetState initState) {
        List<ConnectorTaskId> taskIds = stateConfig.tasks(connector);
        for (ConnectorTaskId taskId : taskIds) {
            worker.startTask(taskId, stateConfig.taskConfig(taskId), initState, this.taskStatusListener);
        }
    }

    private Plugins plugins() {
        return this.worker.plugins();
    }

    @Override
    public void putTaskConfig(
            String connectorName, List<Map<String, String>> configs, CallBack<List<TaskInfo>> callBack) {
        callBack.onCompletion(
                new UnsupportedOperationException(
                        "OMS Connect in standalone mode does not support externally setting task configurations"),
                null);
    }

    private boolean startConnector(String connectorName, Map<String, String> config) {
        configStorageService.putConnectorConfig(connectorName, config);
        TargetState targetState = stateConfig.targetState(connectorName);
        worker.startConnector(connectorName, config, targetState, this.connectorStatusListener);
        return true;
    }

    @Override
    public void deleteConnectorConfig(String connector, CallBack<ConnectorInfo> callBack) {
        if (!stateConfig.contains(connector)) {
            callBack.onCompletion(
                    new ConnectException("Cannot delete a connector that does not exist ; " + connector),
                    null);
            return;
        }
        ConnectorInfo connectorInfo = createConnectorInfo(connector);
        removeConnectorTask(connector);
        for (ConnectorTaskId taskId : stateConfig.tasks(connector)) {
            taskStatusListener.onDeletion(taskId);
        }
        removeConnector(connector);
        connectorStatusListener.onDeletion(connector);
        callBack.onCompletion(null, connectorInfo);
    }

    private void removeConnector(String connector) {
        worker.stopConnector(connector);
        configStorageService.removeConnectorConfig(connector);
    }

    @Override
    public void connectorConfig(String connector, CallBack<Map<String, String>> callBack) {
        if (!stateConfig.contains(connector)) {
            callBack.onCompletion(
                    new ConnectException("The connector does not exist ; " + connector), null);
            return;
        }
        callBack.onCompletion(null, stateConfig.connectorConfig(connector));
    }

    @Override
    public void taskConfigs(String connector, CallBack<List<TaskInfo>> callBack) {
        if (!stateConfig.contains(connector)) {
            callBack.onCompletion(
                    new ConnectException("The connector does not exist ; " + connector), null);
            return;
        }
        List<TaskInfo> taskInfos = new ArrayList<>();
        for (ConnectorTaskId taskId : stateConfig.tasks(connector)) {
            taskInfos.add(new TaskInfo(taskId, stateConfig.taskConfig(taskId)));
        }
        callBack.onCompletion(null, taskInfos);
    }

    @Override
    public void connectorStatus(String connector, CallBack<ConnectorStateInfo> callBack) {
        if (!stateConfig.contains(connector)) {
            callBack.onCompletion(
                    new ConnectException("The connector does not exist ; " + connector), null);
            return;
        }
        List<ConnectorStateInfo.TaskState> taskStates = new ArrayList<>();
        for (ConnectorTaskId taskId : stateConfig.tasks(connector)) {
            taskStates.add(
                    new ConnectorStateInfo.TaskState(
                            taskId, statusStorageService.get(taskId).getState().toString()));
        }
        ConnectorStateInfo connectorStateInfo =
                new ConnectorStateInfo(
                        connector,
                        new ConnectorStateInfo.ConnectorState(
                                connector, statusStorageService.get(connector).getState().toString()),
                        taskStates,
                        getConnectorTypeFromClass(
                                stateConfig
                                        .connectorConfig(connector)
                                        .get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)));
        callBack.onCompletion(null, connectorStateInfo);
    }

    @Override
    public void taskStatus(ConnectorTaskId taskId, CallBack<ConnectorStateInfo.TaskState> callBack) {
        ConnectorStateInfo.TaskState taskState =
                new ConnectorStateInfo.TaskState(
                        taskId, statusStorageService.get(taskId).getState().toString());
        callBack.onCompletion(null, taskState);
    }

    @Override
    public void restartConnector(String connector, CallBack<Void> callBack) {
        if (!stateConfig.contains(connector)) {
            callBack.onCompletion(
                    new ConnectException("The connector does not exist ; " + connector), null);
            return;
        }
        Map<String, String> connectorConfig = stateConfig.connectorConfig(connector);
        TargetState targetState = stateConfig.targetState(connector);
        worker.stopConnector(connector);
        if (worker.startConnector(
                connector, connectorConfig, targetState, this.connectorStatusListener)) {
            callBack.onCompletion(null, null);
        } else {
            callBack.onCompletion(new ConnectException("Failed to start connector: " + connector), null);
        }
    }

    @Override
    public void restartTask(ConnectorTaskId taskId, CallBack<Void> callBack) {
        if (!stateConfig.contains(taskId.getConnectorName())) {
            callBack.onCompletion(
                    new ConnectException("The connector does not exist ; " + taskId.getConnectorName()),
                    null);
            return;
        }
        Map<String, String> taskConfig = stateConfig.taskConfig(taskId);
        TargetState targetState = stateConfig.targetState(taskId.getConnectorName());
        worker.stopAndAwaitTask(taskId);
        if (worker.startTask(taskId, taskConfig, targetState, this.taskStatusListener)) {
            callBack.onCompletion(null, null);

        } else {
            callBack.onCompletion(new ConnectException("Failed to start tast: " + taskId), null);
        }
    }

    @Override
    public void pauseConnector(String connector) {
        if (!stateConfig.contains(connector)) {
            throw new ConnectException("The connector does not exist ; " + connector);
        }
        configStorageService.putTargetState(connector, TargetState.PAUSED);
    }

    @Override
    public void resumeConnector(String connector) {
        if (!stateConfig.contains(connector)) {
            throw new ConnectException("The connector does not exist ; " + connector);
        }
        configStorageService.putTargetState(connector, TargetState.STARTED);
    }

    @Override
    public boolean validateConnectorConfig(Map<String, String> configs) {
        return true;
    }

    private class ConfigChangeListener implements ConfigListener {
        @Override
        public void onConnectorConfigUpdate(String connector) {
            synchronized (StandaloneProcessor.class) {
                stateConfig = configStorageService.snapshot();
            }
        }

        @Override
        public void onConnectorConfigDelete(String connector) {
            synchronized (StandaloneProcessor.class) {
                stateConfig = configStorageService.snapshot();
            }
        }

        @Override
        public void onTaskConfigUpdate(Collection<ConnectorTaskId> taskId) {
            synchronized (StandaloneProcessor.class) {
                stateConfig = configStorageService.snapshot();
            }
        }

        @Override
        public void onConnectorTargerStateUpdate(String connector) {
            synchronized (StandaloneProcessor.class) {
                stateConfig = configStorageService.snapshot();
                TargetState targetState = stateConfig.targetState(connector);
                worker.changeTargetState(targetState, connector);
                if (targetState == TargetState.STARTED) {
                    createOrUpdateTaskConfig(connector);
                }
            }
        }
    }

    public class ConnectorStatusListener implements StatusListener<String> {

        @Override
        public void onStartUp(String connectorOrTaskId) {
            statusStorageService.put(
                    connectorOrTaskId, new ConnectorStatus(connectorOrTaskId, AbstractStatus.State.RUNNING));
        }

        @Override
        public void onPause(String connectorOrTaskId) {
            statusStorageService.put(
                    connectorOrTaskId, new ConnectorStatus(connectorOrTaskId, AbstractStatus.State.PAUSED));
        }

        @Override
        public void onResume(String connectorOrTaskId) {
            statusStorageService.put(
                    connectorOrTaskId, new ConnectorStatus(connectorOrTaskId, AbstractStatus.State.RUNNING));
        }

        @Override
        public void onShutDown(String connectorOrTaskId) {
            statusStorageService.put(
                    connectorOrTaskId,
                    new ConnectorStatus(connectorOrTaskId, AbstractStatus.State.UNASSIGNED));
        }

        @Override
        public void onDeletion(String connectorOrTaskId) {
            statusStorageService.put(
                    connectorOrTaskId,
                    new ConnectorStatus(connectorOrTaskId, AbstractStatus.State.DESTROYED));
        }

        @Override
        public void onFailure(String connectorOrTaskId, Throwable throwable) {
            statusStorageService.put(
                    connectorOrTaskId, new ConnectorStatus(connectorOrTaskId, AbstractStatus.State.FAILED));
        }
    }

    public class TaskStatusListener implements StatusListener<ConnectorTaskId> {

        @Override
        public void onStartUp(ConnectorTaskId connectorOrTaskId) {
            statusStorageService.put(
                    connectorOrTaskId, new TaskStatus(connectorOrTaskId, AbstractStatus.State.RUNNING));
        }

        @Override
        public void onPause(ConnectorTaskId connectorOrTaskId) {
            statusStorageService.put(
                    connectorOrTaskId, new TaskStatus(connectorOrTaskId, AbstractStatus.State.PAUSED));
        }

        @Override
        public void onResume(ConnectorTaskId connectorOrTaskId) {
            statusStorageService.put(
                    connectorOrTaskId, new TaskStatus(connectorOrTaskId, AbstractStatus.State.RUNNING));
        }

        @Override
        public void onShutDown(ConnectorTaskId connectorOrTaskId) {
            statusStorageService.put(
                    connectorOrTaskId, new TaskStatus(connectorOrTaskId, AbstractStatus.State.UNASSIGNED));
        }

        @Override
        public void onDeletion(ConnectorTaskId connectorOrTaskId) {
            statusStorageService.put(
                    connectorOrTaskId, new TaskStatus(connectorOrTaskId, AbstractStatus.State.DESTROYED));
        }

        @Override
        public void onFailure(ConnectorTaskId connectorOrTaskId, Throwable throwable) {
            statusStorageService.put(
                    connectorOrTaskId, new TaskStatus(connectorOrTaskId, AbstractStatus.State.FAILED));
        }
    }
}
