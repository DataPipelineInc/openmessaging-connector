package io.openmessaging.connector.runtime.rest.entities;

import java.util.List;
import java.util.Map;


/**
 * Some configuration used to feed back to the user's connector and task.
 */
public class ConnectorInfo {
    private String connectorName;
    private Map<String, String> connectorConfig;
    private List<ConnectorTaskId> tasks;
    private ConnectorType connectorType;

    public ConnectorInfo(String connectorName, Map<String, String> connectorConfig, List<ConnectorTaskId> tasks, ConnectorType connectorType) {
        this.connectorName = connectorName;
        this.connectorConfig = connectorConfig;
        this.tasks = tasks;
        this.connectorType = connectorType;
    }

    public String getConnectorName() {
        return connectorName;
    }

    public Map<String, String> getConnectorConfig() {
        return connectorConfig;
    }

    public List<ConnectorTaskId> getTasks() {
        return tasks;
    }

    public ConnectorType getConnectorType() {
        return connectorType;
    }
}
