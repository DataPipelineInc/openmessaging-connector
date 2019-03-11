package io.openmessaging.connector.api;

import io.openmessaging.KeyValue;

import java.util.List;

public interface Connector {

    /**
     * Should invoke before start the connector.
     *
     * @return error message
     */
    String verifyAndSetConfig(KeyValue config);

    /**
     * Start the connector with the given config.
     */
    void start();

    /**
     * Stop the connector.
     */
    void stop();

    /**
     * Pause the connector.
     */
    void pause();

    /**
     * Resume the connector.
     */
    void resume();

    /**
     * Returns the Task implementation for this Connector.
     *
     * @return the task implementation.
     */
    Class<? extends Task> taskClass();

    /**
     * Returns a list of configurations for Tasks based on the current configuration.
     *
     * @return a list of configutation.
     */
    List<KeyValue> taskConfigs(int maxTasks);
}
