package io.openmessaging.connector.runtime.utils;

import io.openmessaging.connector.runtime.rest.error.ConnectException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureCallBack<T> implements CallBack<T>, Future<T> {
  private T result;
  private CountDownLatch finishDownLatch = new CountDownLatch(1);
  private Throwable throwable;

  @Override
  public void onCompletion(Throwable throwable, T result) {
    this.result = result;
    this.throwable = throwable;
    this.finishDownLatch.countDown();
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return finishDownLatch.getCount() == 0;
  }

  @Override
  public T get() throws InterruptedException {
    this.finishDownLatch.await();
    return result();
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
    if (!finishDownLatch.await(timeout, unit)) {
      throw new TimeoutException("Timed out waiting for future");
    }
    return result();
  }

  private T result() {
    if (throwable != null) {
      throw new ConnectException(throwable);
    }
    return result;
  }
}
