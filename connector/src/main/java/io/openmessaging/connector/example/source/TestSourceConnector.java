package io.openmessaging.connector.example.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.source.SourceConnector;
import io.openmessaging.connector.runtime.TaskConfig;
import io.openmessaging.internal.DefaultKeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TestSourceConnector extends SourceConnector {
  private static final Logger log = LoggerFactory.getLogger(TestSourceConnector.class);

  @Override
  public String verifyAndSetConfig(KeyValue config) {
    return null;
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
  public List<KeyValue> taskConfigs() {
    List<KeyValue> lists = new ArrayList<>();
    KeyValue keyValue = new DefaultKeyValue();
    keyValue.put(TaskConfig.TASK_CLASS_CONFIG, TestSourceTask.class.getName());
    lists.add(keyValue);
    return lists;
  }
}
