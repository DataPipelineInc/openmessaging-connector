package io.openmessaging.connector.runtime;

import io.openmessaging.connector.runtime.rest.entities.ConnectorTaskId;

import java.util.concurrent.CountDownLatch;

public abstract class WorkerTask implements Runnable {
    private ConnectorTaskId taskId;
    private TargetState targetState;
    private boolean stopping;
    private CountDownLatch shutDownLatch = new CountDownLatch(1);
    private StandaloneProcessor.TaskStatusListener listener;

    public WorkerTask(
            ConnectorTaskId taskId,
            TargetState targetState,
            StandaloneProcessor.TaskStatusListener listener) {
        this.taskId = taskId;
        this.targetState = targetState;
        this.listener = listener;
    }

    /**
     * Initialize this worker task.
     */
    public void initialize() {
    }

    @Override
    public void run() {
        try {
            doRun();
            onShutdown();
        } catch (Throwable throwable) {
            onFailure(throwable);
        } finally {
            releaseResource();
            shutDownLatch.countDown();
        }
    }

    private void doRun() throws InterruptedException {
        try {
            if (isStopping()) {
                return;
            }
            if (shouldPause()) {
                onPause();
                if (!awaitUnPause()) {
                    return;
                }
            }
            execute();
        } finally {
            doClose();
        }
    }

    private void doClose() {
    }


    private void triggerStop() {
        synchronized (this) {
            this.stopping = true;
            this.notifyAll();
        }
    }

    /**
     * Change the target state of this worker task.
     *
     * @param targetState the new target state of this worker task.
     */
    public void changeTargetState(TargetState targetState) {
        synchronized (this) {
            if (isStopping()) {
                return;
            }
            this.targetState = targetState;
            this.notifyAll();
        }
    }

    /**
     * Wait for this task to finish stopping.
     *
     * @return true if this task has finished stopping, false otherwise.
     */
    public boolean awaitStop() {
        try {
            this.shutDownLatch.await();
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    public abstract void execute();

    /**
     * Check if this task should be paused.
     *
     * @return true if is task should be paused , false otherwise.
     */
    public boolean shouldPause() {
        return targetState == TargetState.PAUSED;
    }


    /**
     * Await task resumption.
     *
     * @return true if the task has been resumed.
     * @throws InterruptedException exception.
     */
    public boolean awaitUnPause() throws InterruptedException {
        synchronized (this) {
            while (shouldPause()) {
                if (isStopping()) {
                    return false;
                }
                this.wait();
            }
            return true;
        }
    }

    /**
     * Check if this task is stopping.
     *
     * @return true if is task is stopping, false otherwise.
     */
    public boolean isStopping() {
        return stopping;
    }

    /**
     * Stop this task, this doesn't really stop, just mark stopping as true.
     */
    public void stop() {
        this.triggerStop();
    }


    /**
     * When this worker task is closed, we need to release some resources.
     */
    public void releaseResource() {
    }

    public void onShutdown() {
        synchronized (this) {
            this.stop();
            listener.onShutDown(taskId);
        }
    }

    public void onStartUp() {
        listener.onStartUp(taskId);
    }

    protected void onPause() {
        listener.onPause(taskId);
    }

    public void onResume() {
        listener.onResume(taskId);
    }

    public void onFailure(Throwable throwable) {
        listener.onFailure(taskId, throwable);
    }
}
