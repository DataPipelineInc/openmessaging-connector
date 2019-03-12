package io.openmessaging.connector.runtime;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.connector.api.data.DataEntryBuilder;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SinkDataEntry;
import io.openmessaging.connector.api.sink.SinkTask;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.utils.ConvertUtils;
import io.openmessaging.consumer.PullConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkerSinkTask extends WorkerTask {
    private static final Logger log = LoggerFactory.getLogger(WorkerSinkTask.class);
    private Map<ByteBuffer, ByteBuffer> lastCommitPositions;
    private Map<ByteBuffer, ByteBuffer> currentPositions;
    private Map<ByteBuffer, ByteBuffer> origPositions;
    private PullConsumer pullConsumer;
    private WorkerConfig workerConfig;
    private List<SinkDataEntry> toConsume;
    private WorkerSinkTaskContext context;
    private Map<String, String> config;
    private SinkTask sinkTask;

    public WorkerSinkTask(
            ConnectorTaskId taskId,
            SinkTask sinkTask,
            TargetState targetState,
            StandaloneProcessor.TaskStatusListener listener,
            PullConsumer pullConsumer,
            WorkerConfig workerConfig,
            Map<String, String> config) {
        super(taskId, targetState, listener);
        this.lastCommitPositions = new HashMap<>();
        this.currentPositions = new HashMap<>();
        this.origPositions = new HashMap<>();
        this.pullConsumer = pullConsumer;
        this.workerConfig = workerConfig;
        this.sinkTask = sinkTask;
        this.config = config;
        this.toConsume = new ArrayList<>();
        this.context =
                new WorkerSinkTaskContext(pullConsumer, ConvertUtils.mapToKeyValue(config));
    }

    public void initialize() {
    }

    private void initializeAndStartTask() {
        String queue = this.config.get(TaskConfig.TASK_TOPICS_CONFIG);
        if (queue == null) {
            log.warn("There is no queue to attach");
        } else {
            String[] queues = queue.split(",");
            for (String queueName : queues) {
                pullConsumer.attachQueue(queueName);
            }
        }
        KeyValue keyValue = ConvertUtils.mapToKeyValue(this.config);
        sinkTask.initialize(new WorkerSinkTaskContext(this.pullConsumer, keyValue));
        sinkTask.start(keyValue);
    }

    @Override
    public void execute() {
        initializeAndStartTask();
        try {
            while (!isStopping()) {
                processingMessages();
            }
        } catch (Throwable throwable) {
            commitPositions();
        }
    }

    private void rewindPosition() {

    }

    private void processingMessages() {
        rewindPosition();
        Message message = this.pullConsumer.receive();
        convertMessage(message);
    }

    private void convertMessage(Message message) {
        //TODO put message partition and position into origPositions
        Object[] objects = ConvertUtils.getObjectFromBytes(message.getBody(byte[].class), Object[].class);
        String queuename = (String) objects[0];
        ByteBuffer partition = (ByteBuffer) objects[1];
        ByteBuffer position = (ByteBuffer) objects[2];
        Schema schema = (Schema) objects[3];
        DataEntryBuilder builder = new DataEntryBuilder(schema).queue(queuename);
        for (int i = 0; i < schema.getFields().size(); i++) {
            builder.putFiled(schema.getFields().get(i).toString(), objects[i + 4]);
        }
        toConsume.add(builder.buildSinkDataEntry(0L));
    }

    private void deliverMessages() {
        sinkTask.put(toConsume);
        toConsume.clear();
    }

    private void commitPositions() {
    }
}
