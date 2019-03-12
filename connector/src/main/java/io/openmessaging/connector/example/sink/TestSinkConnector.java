package io.openmessaging.connector.example.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.sink.SinkConnector;
import io.openmessaging.connector.runtime.TaskConfig;
import io.openmessaging.internal.DefaultKeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TestSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(TestSinkConnector.class);

    @Override
    public String verifyAndSetConfig(KeyValue config) {
        return null;
    }

    @Override
    public void start() {
        log.info("The sink connector has started");
    }

    @Override
    public void stop() {
        log.info("The sink connector has stopped");
    }

    @Override
    public void pause() {
        log.info("The sink connector has paused");
    }

    @Override
    public void resume() {
        log.info("The sink connector has resumed");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TestSinkTask.class;
    }

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> lists = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            KeyValue keyValue = new DefaultKeyValue();
            keyValue.put(TaskConfig.TASK_CLASS_CONFIG, TestSinkTask.class.getName());
            keyValue.put(TaskConfig.TASK_TOPICS_CONFIG, "q1");
            lists.add(keyValue);
        }
        return lists;
    }
}
