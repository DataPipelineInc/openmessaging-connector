package io.openmessaging.connector.runtime;

import io.openmessaging.KeyValue;
import io.openmessaging.OMS;
import io.openmessaging.connector.runtime.rest.error.ConnectException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The configuration information class of OMS,this class contains all configuration information such
 * as RestServer, messaging system, etc.
 */
public class WorkerConfig {
    public static final String REST_HOSTNAME = "rest.hostname";
    public static final String REST_PORT = "rest.port";
    public static final String OMS_ACCESSPOINT = "oms.accesspoint";
    public static final String POSITION_COMMIT_INTERVAL_MS_CONFIG = "position.commit.interval.ms.config";
    public static final String POSITION_COMMIT_TIMEOUT_MS_CONFIG = "position.commit.timeout.ms.config";
    public static final String TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS_CONFIG = "task.shutdown.graceful.timeout.ms.config";
    private static Set<String> allConfig = new HashSet<>();

    static {
        allConfig.add(REST_HOSTNAME);
        allConfig.add(REST_PORT);
        allConfig.add(OMS_ACCESSPOINT);
    }

    private KeyValue workerConfig = OMS.newKeyValue();

    public WorkerConfig(Map<String, String> config) {
        Set<String> missingConfig = verifyAndPadding(config);
        if (!missingConfig.isEmpty()) {
            throw new ConnectException("Missing some necessary config : " + missingConfig);
        }
    }

    private Set<String> verifyAndPadding(Map<String, String> config) {
        Set<String> missingConfig = new HashSet<>();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            if (allConfig.contains(entry.getKey())) {
                this.workerConfig.put(entry.getKey(), entry.getValue());
            } else {
                missingConfig.add(entry.getKey());
            }
        }
        return missingConfig;
    }

    public KeyValue getWorkerConfig() {
        return workerConfig;
    }
}
