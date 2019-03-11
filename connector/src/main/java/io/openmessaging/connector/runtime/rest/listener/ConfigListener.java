package io.openmessaging.connector.runtime.rest.listener;

import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;

import java.util.Collection;

/**
 * This listener synchronizes to update ConfigStorageService.
 * If we do something with config or target state, that listener will ensure that we get the latest modified snapshot.
 */
public interface ConfigListener {

    /**
     * Invoked when a connector`s configuration has been created or updated.
     *
     * @param connector the connector`s name.
     */
    void onConnectorConfigUpdate(String connector);

    /**
     * Invoked when a connector`s configuration has been deleted.
     *
     * @param connector the connector`s name.
     */
    void onConnectorConfigDelete(String connector);

    /**
     * Invoked when a tast`s configuration has been created or updated.
     *
     * @param taskId the task`s is.
     */
    void onTaskConfigUpdate(Collection<ConnectorTaskId> taskId);

    /**
     * Invoked when the user has set a new target state (e.g. paused).
     *
     * @param connector the connector`s name.
     */
    void onConnectorTargerStateUpdate(String connector);
}
