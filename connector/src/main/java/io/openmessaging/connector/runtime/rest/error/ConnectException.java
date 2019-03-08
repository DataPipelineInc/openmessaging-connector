package io.openmessaging.connector.runtime.rest.error;

public class ConnectException extends RuntimeException {
    public ConnectException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public ConnectException(String message) {
        super(message);
    }

    public ConnectException(Throwable throwable) {
        super(throwable);
    }
}
