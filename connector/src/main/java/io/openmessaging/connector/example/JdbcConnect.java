package io.openmessaging.connector.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.openmessaging.connector.runtime.rest.error.ConnectException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JdbcConnect {
  private static final Logger log = LoggerFactory.getLogger(JdbcConnect.class);
  private HikariConfig hikariConfig;
  private HikariDataSource hikariDataSource;

  public JdbcConnect(JdbcConfig jdbcConfig) {
    log.info(jdbcConfig.toString());
    this.hikariConfig = createConfig(jdbcConfig);
  }

  private HikariConfig createConfig(JdbcConfig jdbcConfig) {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(
        getJdbcUrl(jdbcConfig.getEndpoint(), jdbcConfig.getPort(), jdbcConfig.getDatabase()));
    hikariConfig.setUsername(jdbcConfig.getUsername());
    hikariConfig.setPassword(jdbcConfig.getPassword());
    return hikariConfig;
  }

  public void init() {
    log.info(hikariConfig.getJdbcUrl());
    hikariDataSource = new HikariDataSource(hikariConfig);
  }

  public ResultSet execute(String sql) {
    try (Connection connection = hikariDataSource.getConnection()) {
      Statement statement = connection.createStatement();
      return statement.executeQuery(sql);
    } catch (SQLException e) {
      throw new ConnectException(e);
    }
  }

  public Connection getConnection() throws SQLException {
    return hikariDataSource.getConnection();
  }

  public void close() {
    log.info("close hikari data source");
    if (hikariDataSource != null) {
      hikariDataSource.close();
      hikariDataSource = null;
    }
  }

  public abstract String getJdbcUrl(String endpoint, Integer port, String database);

  public abstract List<String> readColumns(TableId tableId);
  public static class JdbcConfig {
    private String endpoint;
    private Integer port;
    private String database;
    private String username;
    private String password;

    public JdbcConfig(
        String endpoint, Integer port, String database, String username, String password) {
      this.endpoint = endpoint;
      this.port = port;
      this.database = database;
      this.username = username;
      this.password = password;
    }

    public String getEndpoint() {
      return endpoint;
    }

    public Integer getPort() {
      return port;
    }

    public String getDatabase() {
      return database;
    }

    public String getUsername() {
      return username;
    }

    public String getPassword() {
      return password;
    }

    @Override
    public String toString() {
      return String.format(
          "The connect config is "
              + "[endpoint]:[%s], "
              + "[port]:[%s], "
              + "[database]:[%s], "
              + "[username]:[%s], "
              + "[password]:[%s]",
          getEndpoint(), getPort(), getDatabase(), getUsername(), getPassword());
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }

      JdbcConfig that = (JdbcConfig) obj;

      return new EqualsBuilder()
          .append(endpoint, that.endpoint)
          .append(port, that.port)
          .append(database, that.database)
          .append(username, that.username)
          .append(password, that.password)
          .build();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder()
          .append(endpoint)
          .append(port)
          .append(database)
          .append(username)
          .append(password)
          .build();
    }
  }
}
