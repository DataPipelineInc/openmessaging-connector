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

  public void initialize() {}

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

  private void doClose() {}

  private void triggerStop() {
    synchronized (this) {
      this.stopping = true;
      this.notifyAll();
    }
  }

  public void changeTargerState(TargetState targetState) {
    synchronized (this) {
      if (isStopping()) {
        return;
      }
      this.targetState = targetState;
      this.notifyAll();
    }
  }

  public boolean awaitStop() {
    try {
      this.shutDownLatch.await();
      return true;
    } catch (InterruptedException e) {
      return false;
    }
  }

  public abstract void execute();

  public boolean shouldPause() {
    return targetState == TargetState.PAUSED;
  }

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

  public boolean isStopping() {
    return stopping;
  }

  public void stop() {
    this.triggerStop();
  }

  public void releaseResource() {}

  public void onShutdown() {
    synchronized (this) {
      this.triggerStop();
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
