package io.openmessaging.connector.runtime.isolation;

import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.runtime.rest.error.ConnectException;

/**
 * Use this class to achieve connector level jar isolation, all cnnectors and tasks are created by
 * this class.
 */
public class Plugins {

  /**
   * @param className the name of the class to load.
   * @param pluginClass The type of class that needs to be loaded.
   * @param <U> generics.
   * @return the class to be obtained.
   */
  private static <U> Class<? extends U> pluginClass(String className, Class<U> pluginClass) {
    try {
      Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
      if (pluginClass.isAssignableFrom(clazz)) {
        return (Class<? extends U>) clazz;
      }
      throw new ConnectException("Class not fount : " + className);
    } catch (ClassNotFoundException e) {
      throw new ConnectException(e);
    }
  }

  /**
   * Uses the constructor represented by this constructor object to create and initialize a new
   * instance of the constructor's declaring class.
   *
   * @param clazz the class to be obtained.
   * @param <U> generics.
   * @return the instance of the class.
   */
  private static <U> U newPlugin(Class<? extends U> clazz) {
    try {
      return clazz.getDeclaredConstructor().newInstance();
    } catch (NoSuchMethodException e) {
      throw new ConnectException(
          "Could not find a public no-argument constructor for " + clazz.getName(), e);
    } catch (ReflectiveOperationException | RuntimeException e) {
      throw new ConnectException("Could not instantiate class " + clazz.getName(), e);
    }
  }

  /**
   * Create a connector by the name of the class.
   *
   * @param className the name of the class.
   * @return a connector instance.
   */
  public Connector newConnector(String className) {
    Class<? extends Connector> clazz = pluginClass(className, Connector.class);
    return newPlugin(clazz);
  }

  /**
   * Create a task by the name of the class.
   *
   * @param className the name of the class.
   * @return a task instance.
   */
  public Task newTask(String className) {
    Class<? extends Task> clazz = pluginClass(className, Task.class);
    return newPlugin(clazz);
  }
}
