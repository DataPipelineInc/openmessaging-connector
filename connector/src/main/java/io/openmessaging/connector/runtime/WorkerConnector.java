package io.openmessaging.connector.runtime;

import io.openmessaging.connector.api.Connector;
import java.util.Map;

public class WorkerConnector {

  private Connector connector;
  private String connectorName;
  private State state;
  private Map<String, String> config;
  private StandaloneProcessor.ConnectorStatusListener statusListener;

  public WorkerConnector(
      Connector connector,
      String connectorName,
      Map<String, String> config,
      StandaloneProcessor.ConnectorStatusListener statusListener) {
    this.connector = connector;
    this.connectorName = connectorName;
    this.state = State.INIT;
    this.config = config;
    this.statusListener = statusListener;
  }

  /** Initialize this worker connector. */
  public void initialize() {}

  /**
   * Change the target state of this worker connector.
   *
   * @param targetState the new target state of this worker connector.
   */
  public void changeTargetState(TargetState targetState) {
    if (state == State.FAILED) {
      return;
    }
    if (targetState == TargetState.PAUSED) {
      pause();
    } else if (targetState == TargetState.STARTED) {
      if (state == State.INIT) {
        start();
      } else {
        resume();
      }
    } else {
      throw new IllegalArgumentException("Unhandled target state " + targetState);
    }
  }

  /**
   * This method will be called if some operations fail.
   *
   * @param throwable throwable
   */
  public void onFailure(Throwable throwable) {
    statusListener.onFailure(connectorName, throwable);
    this.state = State.FAILED;
  }

  /** Start this worker connector. */
  public void start() {
    if (doStart()) {
      statusListener.onStartUp(connectorName);
    }
  }

  /** Resume this worker connector. */
  public void resume() {
    if (doStart()) {
      statusListener.onResume(connectorName);
    }
  }

  /** Start or resume this worker connector. */
  private boolean doStart() {
    try {
      switch (state) {
        case STARTED:
          return false;
        case INIT:
        case STOPPED:
          connector.start();
          this.state = State.STARTED;
          return true;
        case PAUSED:
          connector.resume();
          this.state = State.STARTED;
          return true;
        default:
          throw new IllegalArgumentException("Cannot start connector in state " + state);
      }
    } catch (Throwable throwable) {
      onFailure(throwable);
      return false;
    }
  }

  /** Pause this worker connector. */
  private void pause() {
    try {
      switch (state) {
        case STOPPED:
        case PAUSED:
          return;
        case STARTED:
          connector.pause();
        case INIT:
          statusListener.onPause(connectorName);
          this.state = State.PAUSED;
          break;
        default:
          throw new IllegalArgumentException("Cannot pause connector in state " + state);
      }
    } catch (Throwable throwable) {
      onFailure(throwable);
    }
  }

  /** Stop this worker connector */
  public void stop() {
    try {
      if (state == State.STARTED || state == State.PAUSED) {
        connector.stop();
        this.state = State.STOPPED;
        statusListener.onShutDown(connectorName);
      }
    } catch (Throwable throwable) {
      onFailure(throwable);
    }
  }

  /**
   * Get this connector.
   *
   * @return this connector.
   */
  public Connector getConnector() {
    return connector;
  }

  private enum State {
    INIT, // initial state before startup
    STOPPED, // the connector has been stopped/paused.
    PAUSED,
    STARTED, // the connector has been started/resumed.
    FAILED, // the connector has failed (no further transitions are possible after this state)
  }
}
