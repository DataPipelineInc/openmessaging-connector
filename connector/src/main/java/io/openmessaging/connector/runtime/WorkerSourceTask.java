package io.openmessaging.connector.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.connector.api.PositionStorageReader;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.storage.PositionStorageWriter;
import io.openmessaging.connector.runtime.utils.ConvertUtils;
import io.openmessaging.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WorkerSourceTask extends WorkerTask {
  private static final Logger log = LoggerFactory.getLogger(WorkerSourceTask.class);
  private Producer producer;
  private SourceTask sourceTask;
  private KeyValue config;
  private WorkerConfig workerConfig;
  private PositionStorageReader positionStorageReader;
  private PositionStorageWriter positionStorageWriter;
  private Collection<SourceDataEntry> toSend;
  private IdentityHashMap<Message, Message> beforeFlushMessage = new IdentityHashMap<>();
  private IdentityHashMap<Message, Message> duringFlushMessage = new IdentityHashMap<>();
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
        if (toSend == null) {
          toSend = sourceTask.poll();
        }
        if (toSend == null) {
          continue;
        }
        sendMessages();
      }
    } catch (InterruptedException ignore) {
    } finally {
      commitPosition();
    }
  }

  private void sendMessages() {
    List<String> thisBatch = new ArrayList<>();
    for (SourceDataEntry dataEntry : toSend) {
      Message message =
          this.producer.createBytesMessage(
              dataEntry.getQueueName(), ConvertUtils.getBytesfromObject(dataEntry.getPayload()));
      byte[] sourcePartition = dataEntry.getSourcePartition();
      byte[] sourcePosition = dataEntry.getSourcePosition();
      try {
        //        this.producer.send(message);
        thisBatch.add(
            Arrays.stream(
                    new ObjectMapper().readValue(message.getBody(byte[].class), Object[].class))
                .map(Object::toString)
                .collect(Collectors.joining("_")));
      } catch (Throwable throwable) {
        log.warn("{} Failed to send {}", this, message, throwable);
      }
    }
    log.info("Success to send message");
    log.info("Message is :{}", String.join(" | ", thisBatch));
    toSend = null;
  }

  public void commitPosition() {}

  @Override
  public void onStartUp() {
    super.onStartUp();
    sourceTask.start(config);
  }

  @Override
  protected void onPause() {
    super.onPause();
    sourceTask.pause();
  }

  @Override
  public void onResume() {
    super.onResume();
    sourceTask.resume();
  }

  @Override
  public void onShutdown() {
    super.onShutdown();
    sourceTask.stop();
  }
}
