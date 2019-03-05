package io.openmessaging.connector.example.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class TestSourceTask extends SourceTask {
  private static final Logger log = LoggerFactory.getLogger(TestSourceTask.class);

  @Override
  public Collection<SourceDataEntry> poll() {

    return null;
  }

  @Override
  public void start(KeyValue config) {
    log.info("This task has started");
  }

  @Override
  public void stop() {
    log.info("This task has stopped");
  }

  @Override
  public void pause() {
    log.info("This task has paused");
  }

  @Override
  public void resume() {
    log.info("This task has resumed");
  }
}
