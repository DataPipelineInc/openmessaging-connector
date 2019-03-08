package io.openmessaging.connector.runtime;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.sink.SinkTaskContext;
import io.openmessaging.consumer.PullConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkerSinkTaskContext implements SinkTaskContext {
  private static final Logger log = LoggerFactory.getLogger(WorkerSinkTaskContext.class);
  private PullConsumer consumer;
  private Map<String, Long> offsets;
  private KeyValue keyValue;

  public WorkerSinkTaskContext(PullConsumer consumer, KeyValue keyValue) {
    this.consumer = consumer;
    this.keyValue = keyValue;
    this.offsets = new HashMap<>();
  }

  @Override
  public void resetOffset(String queueName, Long offset) {
    log.error("Not support");
  }

  @Override
  public void resetOffset(Map<String, Long> offsets) {
    log.error("Not support");
  }

  @Override
  public void pause(List<String> queueName) {
    log.error("Not support");
  }

  @Override
  public void resume(List<String> queueName) {
    log.error("Not support");
  }

  @Override
  public KeyValue configs() {
    return keyValue;
  }

  public Map<String, Long> offsets() {
    return offsets;
  }
}
