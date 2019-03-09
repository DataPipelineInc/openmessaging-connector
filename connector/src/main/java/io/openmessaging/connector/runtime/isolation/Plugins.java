package io.openmessaging.connector.runtime.isolation;

import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.runtime.rest.error.ConnectException;


/**
 * Use this class to achieve connector level jar isolation,
 * all cnnectors and tasks are created by this class.
 */
public class Plugins {

    /**
     *
     * @param className
     * @param pluginClass
     * @param <U>
     * @return
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

    public Connector newConnector(String className) {
        Class<? extends Connector> clazz = pluginClass(className, Connector.class);
        return newPlugin(clazz);
    }

    public Task newTask(String className) {
        Class<? extends Task> clazz = pluginClass(className, Task.class);
        return newPlugin(clazz);
    }
}
