package io.openmessaging.connector.runtime;

import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.runtime.utils.ConvertUtils;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class MessageBody {
  private String topic;
  private String partition;
  private Map<String, Object> position;
  private Schema schema;
  private Object[] payload;

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getPartition() {
    return partition;
  }

  public void setPartition(String partition) {
    this.partition = partition;
  }

  public Map<String, Object> getPosition() {
    return position;
  }

  public void setPosition(Map<String, Object> position) {
    this.position = position;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public Object[] getPayload() {
    return payload;
  }

  public void setPayload(Object[] payload) {
    this.payload = payload;
  }

  @Override
  public String toString() {
    return String.format(
        "Topic : %s , Partition : %s , Position : %s , Schema : %s , Payload : %s ",
        getTopic(),
        getPartition(),
        ConvertUtils.getJsonString(getPosition()),
        ConvertUtils.getJsonString(getSchema()),
        Arrays.stream(getPayload()).map(Object::toString).collect(Collectors.joining(" | ")));
  }
}
