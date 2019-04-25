package io.openmessaging.connector.example.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.source.SourceConnector;
import io.openmessaging.connector.example.JdbcConfigKeys;
import io.openmessaging.connector.runtime.TaskConfig;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSourceConnector extends SourceConnector {
  private static final Logger log = LoggerFactory.getLogger(TestSourceConnector.class);
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
      config.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
      return null;
    } else {
      return String.format("The following keys is missing : ", missingKeys.toString());
    }
  }

  @Override
  public void start() {
    log.info("This connector has started");
  }

  @Override
  public void stop() {
    log.info("This connector has stoped");
  }

  @Override
  public void pause() {
    log.info("This connector has paused");
  }

  @Override
  public void resume() {
    log.info("This connector has resumed");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return TestSourceTask.class;
  }

  @Override
  public List<KeyValue> taskConfigs(int maxTasks) {
    List<KeyValue> lists = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      lists.add(config);
    }
    return lists;
  }
}
