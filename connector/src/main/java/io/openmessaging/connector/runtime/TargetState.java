package io.openmessaging.connector.runtime;

/**
 * The targetState of a Connector is changed by the Rest API. When a Connector is first created, the
 * targetState of the Connector is STARTED, which means that the framework will start the Connector.
 * If the targetState of the Connector is PAUSED, the framework will try to pause the Connector.
 */
public enum TargetState {
  STARTED,
  PAUSED
}
