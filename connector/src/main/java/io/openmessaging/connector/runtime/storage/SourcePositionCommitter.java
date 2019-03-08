package io.openmessaging.connector.runtime.storage;

import io.openmessaging.connector.runtime.WorkerConfig;
import io.openmessaging.connector.runtime.WorkerSourceTask;
import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;
import io.openmessaging.connector.runtime.rest.error.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

public class SourcePositionCommitter {
    private static final Logger log = LoggerFactory.getLogger(SourcePositionCommitter.class);
    private ScheduledExecutorService executorService;
    private Map<ConnectorTaskId, ScheduledFuture> committers;
    private WorkerConfig workerConfig;

    public SourcePositionCommitter(WorkerConfig workerConfig) {
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.committers = new ConcurrentHashMap<>();
        this.workerConfig = workerConfig;
    }

    public void schedule(ConnectorTaskId taskId, WorkerSourceTask workerSourceTask) {
        ScheduledFuture future =
                this.executorService.scheduleWithFixedDelay(
                        workerSourceTask::commitPosition, 5, 5, TimeUnit.SECONDS);
        committers.put(taskId, future);
    }

    public void remove(ConnectorTaskId taskId) {
        ScheduledFuture future = committers.remove(taskId);
        if (future == null) {
            return;
        }
        // If we delete this task, we should ensure that the task being performed is completed or
        // cancels the task that has not yet started.
        try {
            future.cancel(false);
            if (!future.isDone()) {
                future.get();
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new ConnectException(
                    "Unexpected interruption in SourcePositionCommitter while removing task with id: "
                            + taskId,
                    e);
        }
    }

    public void shutdowm(long timeout) {
        this.executorService.shutdown();
        try {
            executorService.awaitTermination(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Graceful shutdown of offset commitOffsets thread timed out.");
        }
    }
}
