package io.openmessaging.connector.example;

import java.sql.ResultSet;

public interface JdbcResultSetConsumer {
  void accept(ResultSet resultSet);
}
