package io.openmessaging.connector.runtime.rest.entities;

public class MessageSystem {
  private String messageSystemName;
  private String messageSystemversion;

  public MessageSystem(String messageSystemName, String messageSystemversion) {
    this.messageSystemName = messageSystemName;
    this.messageSystemversion = messageSystemversion;
  }

  public String getMessageSystemName() {
    return messageSystemName;
  }

  public String getMessageSystemversion() {
    return messageSystemversion;
  }
}
