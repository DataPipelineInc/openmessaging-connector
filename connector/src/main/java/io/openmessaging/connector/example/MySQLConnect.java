package io.openmessaging.connector.example;

import io.openmessaging.connector.runtime.rest.error.ConnectException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MySQLConnect extends JdbcConnect {

  public MySQLConnect(JdbcConfig jdbcConfig) {
    super(jdbcConfig);
  }

  @Override
  public String getJdbcUrl(String endpoint, Integer port, String database) {
    return String.format(
        "jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf-8&useSSL=false&autoReconnect=true&allowMultiQueries=true",
        endpoint, port, database);
  }

  public List<String> readColumns(TableId tableId) {
    List<String> columns = new ArrayList<>();
    try {
      DatabaseMetaData databaseMetaData = getConnection().getMetaData();
      ResultSet rs = databaseMetaData.getColumns(null, null, tableId.toString(), null);
      while (rs.next()) {
        columns.add(rs.getString("COLUMN_NAME"));
      }
    } catch (SQLException e) {
      throw new ConnectException(e);
    }
    return columns;
  }
}
