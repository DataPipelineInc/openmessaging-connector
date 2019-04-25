package io.openmessaging.connector.runtime.rest.entities;

import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.api.sink.SinkConnector;
import io.openmessaging.connector.api.source.SourceConnector;

public enum ConnectorType {
  SOURCE,
  SINK,
  UNKNOWN;

  public static ConnectorType fromClass(Class<? extends Connector> clazz) {
    if (SourceConnector.class.isAssignableFrom(clazz)) {
      return SOURCE;
    }
    if (SinkConnector.class.isAssignableFrom(clazz)) {
      return SINK;
    }
    return UNKNOWN;
  }
}
