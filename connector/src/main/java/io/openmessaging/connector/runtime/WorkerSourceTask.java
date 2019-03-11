package io.openmessaging.connector.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.connector.api.PositionStorageReader;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.error.ConnectException;
import io.openmessaging.connector.runtime.storage.PositionStorageWriter;
import io.openmessaging.connector.runtime.utils.ConvertUtils;
import io.openmessaging.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * WorkerTask on the source side，this worker is responsible for writing data to the messaging system.
 */
public class WorkerSourceTask extends WorkerTask {
    private static final Logger log = LoggerFactory.getLogger(WorkerSourceTask.class);
    private Producer producer;
    private SourceTask sourceTask;
    private KeyValue config;
    private WorkerConfig workerConfig;
    private PositionStorageReader positionStorageReader;
    private PositionStorageWriter positionStorageWriter;
    private List<SourceDataEntry> toSend = new ArrayList<>();
    private boolean lastFailed;
    private IdentityHashMap<SourceDataEntry, SourceDataEntry> beforeFlushMessage =
            new IdentityHashMap<>();
    private IdentityHashMap<SourceDataEntry, SourceDataEntry> duringFlushMessage =
            new IdentityHashMap<>();
    private boolean flushing;

    public WorkerSourceTask(
            ConnectorTaskId taskId,
            SourceTask sourceTask,
            TargetState targetState,
            StandaloneProcessor.TaskStatusListener listener,
            WorkerConfig workerConfig,
            PositionStorageReader positionStorageReader,
            PositionStorageWriter positionStorageWriter,
            Map<String, String> config,
            Producer producer) {
        super(taskId, targetState, listener);
        this.workerConfig = workerConfig;
        this.positionStorageReader = positionStorageReader;
        this.positionStorageWriter = positionStorageWriter;
        this.sourceTask = sourceTask;
        this.config = ConvertUtils.mapToKeyValue(config);
        this.producer = producer;
        this.lastFailed = false;
    }


    public void initialize() {
        super.initialize();
        this.producer.startup();
    }

    @Override
    public void execute() {
        try {
            sourceTask.initialize(new WorkerSourceTaskContext(config, this.positionStorageReader));
            onStartUp();
            while (!isStopping()) {
                if (shouldPause()) {
                    onPause();
                    if (awaitUnPause()) {
                        onResume();
                    }
                    continue;
                }
                if (toSend.isEmpty()) {
                    toSend = new ArrayList<>(sourceTask.poll());
                }
                if (toSend.isEmpty()) {
                    continue;
                }
                if (!sendMessages()) {
                    log.info("Failed to send this message batch");
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException ignore) {
        } finally {
            commitPosition();
        }
    }

    /**
     * This method will send a batch of message to the messaging system, and if it fails, it can be resent.
     *
     * @return true if all message has been send successfully ,false otherwise.
     */
    private boolean sendMessages() {
        int processed = 0;
        List<String> thisBatch = new ArrayList<>();
        for (SourceDataEntry dataEntry : toSend) {
            Message message =
                    this.producer.createBytesMessage(
                            dataEntry.getQueueName(), ConvertUtils.getBytesfromObject(dataEntry.getPayload()));
            ByteBuffer sourcePartition = dataEntry.getSourcePartition();
            ByteBuffer sourcePosition = dataEntry.getSourcePosition();
            //If we fail to send, we will not re-add the failed message to the map, but send it directly again.
            synchronized (this) {
                if (!lastFailed) {
                    if (flushing) {
                        duringFlushMessage.put(dataEntry, dataEntry);
                    } else {
                        beforeFlushMessage.put(dataEntry, dataEntry);
                    }
                    positionStorageWriter.position(sourcePartition, sourcePosition);
                }
            }
            try {
                //TODO Asynchronously send a message, according to callBack to determine whether the message is sent successfully.
                this.producer.send(message);
                commitMessage(dataEntry);
                processed++;
                lastFailed = false;
                thisBatch.add(
                        Arrays.stream(
                                new ObjectMapper().readValue(message.getBody(byte[].class), Object[].class))
                                .map(Object::toString)
                                .collect(Collectors.joining("_")));
            } catch (Throwable throwable) {
                log.warn("{} Failed to send {}", this, message, throwable);
                if (lastFailed = true) {
                    throw new ConnectException("Resend failed", throwable);
                }
                lastFailed = true;
                toSend = toSend.subList(processed, toSend.size());
                return false;
            }
        }
        log.info("Success to send message");
        log.info("Message is :{}", String.join(" | ", thisBatch));
        toSend.clear();
        return true;
    }

    /**
     * If the message is sent successfully, we will remove the message from the map.
     *
     * @param sourceDataEntry the source data.
     */
    private synchronized void commitMessage(SourceDataEntry sourceDataEntry) {
        SourceDataEntry dataEntry = beforeFlushMessage.remove(sourceDataEntry);
        if (dataEntry == null) {
            dataEntry = duringFlushMessage.remove(sourceDataEntry);
        }
        if (dataEntry == null) {
            log.error(
                    "{} Critical saw callback for message that was not present in the all message sets: {}",
                    this,
                    dataEntry);
        } else if (flushing && beforeFlushMessage.isEmpty()) {
            //Flush's thread will wait, only the messages in this map will be successfully sent.
            this.notifyAll();
        }
    }

    /**
     * Flush
     */
    public void commitPosition() {
        log.info("Start flush");
        long commitTimeLimit = this.workerConfig.getWorkerConfig().getLong(WorkerConfig.POSITION_COMMIT_TIMEOUT_MS_CONFIG);
        synchronized (this) {
            this.flushing = true;
            boolean prepare = this.positionStorageWriter.beforeFlush();

            // Wait for the Message in the beforeFlushMessage queue to be successfully sent

            if (!beforeFlushMessage.isEmpty()) {
                try {
                    this.wait(commitTimeLimit);
                } catch (InterruptedException exception) {
                    log.error(
                            "{} Interrupted while flushing messages, positions will not be committed", this);
                    onFailedFlush();
                    return;
                }
            }
            if (!prepare) {
                log.info("There is no data in positionStorageWriter");
                onSuccessfulFlush();
                return;
            }
            Future flushFuture =
                    positionStorageWriter.doFlush(
                            (throwable, result) -> {
                                if (throwable != null) {
                                    log.error("{} Failed to flush positions to storage: ", this, throwable);
                                } else {
                                    log.info("{} Finished flushing positions to storage", this);
                                }
                            });
            try {
                flushFuture.get();
            } catch (InterruptedException exception) {
                log.warn("{} Flush of positions interrupted, cancelling", this);
                onFailedFlush();
            } catch (ExecutionException exception) {
                log.error("{} Flush of positions threw an unexpected exception: ", this, exception);
                onFailedFlush();
            }
            onSuccessfulFlush();
        }
    }

    /**
     * Flush successfully.
     */
    private void onSuccessfulFlush() {
        beforeFlushMessage.putAll(duringFlushMessage);
        duringFlushMessage.clear();
        flushing = false;
    }

    /**
     * Flush Failed.
     */
    private void onFailedFlush() {
        positionStorageWriter.onFailed();
        beforeFlushMessage.putAll(duringFlushMessage);
        duringFlushMessage.clear();
        flushing = false;
    }

    @Override
    public void onStartUp() {
        sourceTask.start(config);
        super.onStartUp();
    }

    @Override
    protected void onPause() {
        sourceTask.pause();
        super.onPause();
    }

    @Override
    public void onResume() {
        sourceTask.resume();
        super.onResume();
    }

    @Override
    public void onShutdown() {
        sourceTask.stop();
        super.onShutdown();
    }
}
