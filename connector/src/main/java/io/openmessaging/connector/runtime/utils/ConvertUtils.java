package io.openmessaging.connector.runtime.utils;

import io.openmessaging.KeyValue;
import io.openmessaging.OMS;

import java.util.HashMap;
import java.util.Map;

public class ConvertUtils {
  public static Map<String, String> keyValueToMap(KeyValue keyValue) {
    Map<String, String> map = new HashMap<>();
    for (String key : keyValue.keySet()) {
      map.put(key, keyValue.getString(key));
    }
    return map;
  }

  public static KeyValue mapToKeyValue(Map<String, String> map) {
    KeyValue keyValue = OMS.newKeyValue();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      keyValue.put(entry.getKey(), entry.getValue());
    }
    return keyValue;
  }
}
