package io.openmessaging.connector.example.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.DataEntryBuilder;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.internal.DefaultKeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TestSourceTask extends SourceTask {
  private static final Logger log = LoggerFactory.getLogger(TestSourceTask.class);
  private int i = 0;

  @Override
  public Collection<SourceDataEntry> poll() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field(0, "id", FieldType.STRING));
    Schema schema = new Schema();
    schema.setFields(fields);
    int index = 0;
    List<SourceDataEntry> lists = new ArrayList<>();
    while (index != 5) {
      lists.add(
          new DataEntryBuilder(schema)
              .queue("q1")
              .putFiled("id", i++)
              .buildSourceDataEntry("partition01".getBytes(), ("position" + i).getBytes()));
      index++;
    }
    return lists;
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
