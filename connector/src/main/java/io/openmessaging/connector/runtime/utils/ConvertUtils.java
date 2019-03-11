package io.openmessaging.connector.runtime.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openmessaging.KeyValue;
import io.openmessaging.OMS;
import io.openmessaging.connector.runtime.rest.error.ConnectException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConvertUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper();

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
        try {
            return objectMapper.writeValueAsBytes(o);
        } catch (JsonProcessingException e) {
            throw new ConnectException(e);
        }
    }

    public static <T> T getObjectFromBytes(byte[] bytes, Class<T> clazz) {
        try {
            return objectMapper.readValue(bytes, clazz);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }


    public static Map<String, String> propertiesToMap(Properties properties) {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            map.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return map;
    }
}
