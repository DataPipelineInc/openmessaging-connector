package io.openmessaging.connector.runtime;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.PositionStorageReader;
import io.openmessaging.connector.api.source.SourceTaskContext;

public class WorkerSourceTaskContext implements SourceTaskContext {
    private PositionStorageReader positionStorageReader;
    private KeyValue keyValue;

    public WorkerSourceTaskContext(KeyValue keyValue, PositionStorageReader positionStorageReader) {
        this.positionStorageReader = positionStorageReader;
        this.keyValue = keyValue;
    }

    @Override
    public PositionStorageReader offsetStorageReader() {
        return this.positionStorageReader;
    }

    @Override
    public KeyValue configs() {
        return this.keyValue;
    }
}
