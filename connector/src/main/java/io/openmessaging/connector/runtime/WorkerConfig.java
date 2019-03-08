package io.openmessaging.connector.runtime;

import io.openmessaging.KeyValue;

/**
 * The configuration information class of OMS,this class contains all configuration information such
 * as RestServer, messaging system, etc.
 */
public class WorkerConfig {
    public static final String REST_HOSTNAME = "rest.hostname";
    public static final String REST_PORT = "rest.port";
    public static final String OMS_ACCESSPOINT = "oms.accesspoint";
    private KeyValue restConfig;
    private KeyValue messagingSystemConfig;
    private KeyValue otherConfig;
    private KeyValue taskConfig;
    private KeyValue connectorConfig;

    public WorkerConfig(
            KeyValue restConfig,
            KeyValue messagingSystemConfig,
            KeyValue otherConfig,
            KeyValue taskConfig,
            KeyValue connectorConfig) {
        this.restConfig = restConfig;
        this.messagingSystemConfig = messagingSystemConfig;
        this.otherConfig = otherConfig;
        this.taskConfig = taskConfig;
        this.connectorConfig = connectorConfig;
    }

    public KeyValue getRestConfig() {
        return restConfig;
    }

    public KeyValue getMessagingSystemConfig() {
        return messagingSystemConfig;
    }

    public KeyValue getOtherConfig() {
        return otherConfig;
    }

    public KeyValue getTaskConfig() {
        return taskConfig;
    }

    public KeyValue getConnectorConfig() {
        return connectorConfig;
    }
}
