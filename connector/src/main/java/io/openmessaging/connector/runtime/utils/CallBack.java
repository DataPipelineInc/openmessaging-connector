package io.openmessaging.connector.runtime.utils;

public interface CallBack<T> {
  void onCompletion(Throwable throwable, T result);
}
