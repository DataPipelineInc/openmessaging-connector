package io.openmessaging.connector.runtime.rest.storage;

import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.rest.entities.ConnectorStatus;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.entities.TaskStatus;

/**
 * Status is an interface that saves the connector and task status.After the state of the connector
 * or task is successfully changed, the method of this interface is called to save the latest state
 * to the memory or the location specified by the user.
 */
public interface StatusStorageService {

    /**
     * Start the status storage service.
     */
    void start();

    /**
     * Stop the status storage service.
     */
    void stop();

    /**
     * Config the status storage service with the given config.
     *
     * @param workerConfig the config of worker.
     */
    void initialize(WorkerConfig workerConfig);

    /**
     * Get the current status of the connector.
     *
     * @param connectorName the name of the connector.
     * @return the status of the connector.
     */
    ConnectorStatus get(String connectorName);

    /**
     * Get the current status of the task.
     *
     * @param taskId the id of the task.
     * @return the status of the task.
     */
    TaskStatus get(ConnectorTaskId taskId);

    /**
     * Set the state of the connector to the given value.
     *
     * @param connectorName   the connector`s name.
     * @param connectorStatus the status of the connector.
     */
    void put(String connectorName, ConnectorStatus connectorStatus);

    /**
     * Set the state of the tast to the given value.
     *
     * @param taskId     the task`s id.
     * @param taskStatus the status of the task.
     */
    void put(ConnectorTaskId taskId, TaskStatus taskStatus);
}
