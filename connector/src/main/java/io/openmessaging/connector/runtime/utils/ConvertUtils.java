package io.openmessaging.connector.runtime.utils;

import com.alibaba.fastjson.JSON;
import io.openmessaging.KeyValue;
import io.openmessaging.OMS;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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

  public static byte[] getBytesfromObject(Object o) {
    return JSON.toJSONBytes(o);
  }

  public static <T> T getObjectFromBytes(byte[] bytes, Class<T> clazz) {
    return JSON.parseObject(bytes, clazz);
  }

  public static Map<String, String> propertiesToMap(Properties properties) {
    Map<String, String> map = new HashMap<>();
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      map.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return map;
  }

  public static String getJsonString(Object object) {
    return JSON.toJSONString(object);
  }
}
