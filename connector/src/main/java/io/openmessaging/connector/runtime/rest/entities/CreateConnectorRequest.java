package io.openmessaging.connector.runtime.rest.entities;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import java.util.Map;

public class CreateConnectorRequest {
  private String name;
  private Map<String, String> config;

  @JSONCreator
  public CreateConnectorRequest(@JSONField String name, @JSONField Map<String, String> config) {
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
