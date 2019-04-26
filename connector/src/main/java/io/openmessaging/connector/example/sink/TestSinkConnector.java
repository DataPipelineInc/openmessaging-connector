package io.openmessaging.connector.example.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.sink.SinkConnector;
import io.openmessaging.connector.example.JdbcConfigKeys;
import io.openmessaging.connector.runtime.TaskConfig;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(TestSinkConnector.class);

  private KeyValue config;
  private Set<String> keys = new HashSet<>();

  {
    keys.add(JdbcConfigKeys.JDBC_ENDPOINT);
    keys.add(JdbcConfigKeys.JDBC_PORT);
    keys.add(JdbcConfigKeys.JDBC_DATABASE);
    keys.add(JdbcConfigKeys.JDBC_USERNAME);
    keys.add(JdbcConfigKeys.JDBC_PASSWORD);
    keys.add(JdbcConfigKeys.TABLE_NAME);
    keys.add(JdbcConfigKeys.ORDER_COLUMN);
    keys.add(TaskConfig.TASK_TOPICS_CONFIG);
  }

  @Override
  public String verifyAndSetConfig(KeyValue config) {
    Set<String> missingKeys = new HashSet<>();
    this.config = config;
    keys.stream()
        .forEach(
            key -> {
              if (!config.containsKey(key)) {
                missingKeys.add(key);
              }
            });
    if (missingKeys.isEmpty()) {
      this.config.put(TaskConfig.TASK_CLASS_CONFIG, TestSinkTask.class.getName());
      return null;
    } else {
      return String.format("The following keys is missing : ", missingKeys.toString());
    }
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
      lists.add(this.config);
    }
    return lists;
  }
}
