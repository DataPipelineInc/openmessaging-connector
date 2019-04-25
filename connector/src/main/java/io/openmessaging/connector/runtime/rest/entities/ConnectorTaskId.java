package io.openmessaging.connector.runtime.rest.entities;

/**
 * The unique ID of the task,it includes a unique connector ID and a task ID that is unique within
 * the connector.
 */
public class ConnectorTaskId implements Comparable<ConnectorTaskId> {
  private String connector;
  private int id;

  public ConnectorTaskId(String connector, int id) {
    this.connector = connector;
    this.id = id;
  }

  public String getConnectorName() {
    return connector;
  }

  public int getTaskId() {
    return id;
  }

  @Override
  public String toString() {
    return connector + '-' + id;
  }

  @Override
  public int hashCode() {
    int result = connector != null ? connector.hashCode() : 0;
    result = 31 * result + id;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ConnectorTaskId that = (ConnectorTaskId) o;

    if (id != that.id) return false;
    if (connector != null ? !connector.equals(that.connector) : that.connector != null)
      return false;

    return true;
  }

  @Override
  public int compareTo(ConnectorTaskId o) {
    int connectorCmp = connector.compareTo(o.connector);
    if (connectorCmp != 0) return connectorCmp;
    return Integer.compare(id, o.id);
  }
}
