package io.openmessaging.connector.example.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.SinkDataEntry;
import io.openmessaging.connector.api.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class TestSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(TestSinkTask.class);

    @Override
    public void put(Collection<SinkDataEntry> sinkDataEntries) {
        for (SinkDataEntry entry : sinkDataEntries) {
            String result = Arrays.stream(entry.getPayload()).map(Object::toString).collect(Collectors.joining("_"));
            log.info("The result is : {}", result);
        }
    }

    @Override
    public void start(KeyValue config) {
        log.info("The sink task has started");
    }

    @Override
    public void stop() {
        log.info("The sink task has stopped");
    }

    @Override
    public void pause() {
        log.info("The sink task has paused");
    }

    @Override
    public void resume() {
        log.info("The sink task has resumed");
    }
}
