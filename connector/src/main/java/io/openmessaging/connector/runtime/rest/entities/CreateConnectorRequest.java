package io.openmessaging.connector.runtime.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class CreateConnectorRequest {
  private String name;
  private Map<String, String> config;

  @JsonCreator
  public CreateConnectorRequest(
      @JsonProperty("name") String name, @JsonProperty("config") Map<String, String> config) {
    this.name = name;
    this.config = config;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getConfig() {
    return config;
  }
}
