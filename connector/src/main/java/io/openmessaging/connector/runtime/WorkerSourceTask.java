package io.openmessaging.connector.runtime;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.connector.api.PositionStorageReader;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.listener.TaskStatusListener;
import io.openmessaging.connector.runtime.storage.PositionStorageWriter;
import io.openmessaging.connector.runtime.utils.ConvertUtils;
import io.openmessaging.producer.Producer;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;

public class WorkerSourceTask extends WorkerTask {
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
      TaskStatusListener listener,
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

  @Override
  public void execute() {
    try {
      sourceTask.initialize(new WorkerSourceTaskContext(config, this.positionStorageReader));
      sourceTask.start(config);
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
    for(SourceDataEntry dataEntry:toSend){
      Message message = this.producer.createBytesMessage()
    }
  }

  public void commitPosition() {}
}
