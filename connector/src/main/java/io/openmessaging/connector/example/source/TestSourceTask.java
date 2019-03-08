package io.openmessaging.connector.example.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.*;
import io.openmessaging.connector.api.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
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
                            .buildSourceDataEntry(ByteBuffer.wrap("partition01".getBytes()), ByteBuffer.wrap(("position" + i).getBytes())));
            index++;
        }
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
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
