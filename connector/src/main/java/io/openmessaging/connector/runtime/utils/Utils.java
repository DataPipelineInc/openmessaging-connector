package io.openmessaging.connector.runtime.utils;


import io.openmessaging.connector.runtime.rest.error.ConnectException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Utils {
    public static Properties getProperties(String filename) {
        try {
            FileInputStream fileInputStream = new FileInputStream(filename);
            Properties properties = new Properties();
            properties.load(fileInputStream);
            return properties;
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }
}
