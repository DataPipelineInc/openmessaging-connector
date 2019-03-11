package io.openmessaging.connector.runtime;

import io.openmessaging.Message;
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
    private PullConsumer pullConsumer;
    private WorkerConfig workerConfig;
    private List<Message> toConsume;
    private WorkerSinkTaskContext context;
    private Map<String, String> config;

    public WorkerSinkTask(
            ConnectorTaskId taskId,
            TargetState targetState,
            StandaloneProcessor.TaskStatusListener listener,
            PullConsumer pullConsumer,
            WorkerConfig workerConfig,
            Map<String, String> config) {
        super(taskId, targetState, listener);
        this.lastCommitPositions = new HashMap<>();
        this.currentPositions = new HashMap<>();
        this.pullConsumer = pullConsumer;
        this.workerConfig = workerConfig;
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
        Map<String, Long> offset = context.offsets();

    }

    private void processingMessages() {
        rewindPosition();

    }

    private void deliverMessages() {
    }

    private void commitPositions() {
    }
}
