package io.openmessaging.connector.example.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SinkDataEntry;
import io.openmessaging.connector.api.sink.SinkTask;
import io.openmessaging.connector.example.JdbcConfigKeys;
import io.openmessaging.connector.example.JdbcConnect;
import io.openmessaging.connector.example.JdbcConnect.JdbcConfig;
import io.openmessaging.connector.example.MySQLConnect;
import io.openmessaging.connector.example.TableId;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("Duplicates")
public class TestSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(TestSinkTask.class);
  private KeyValue keyValue;
  private JdbcConnect jdbcConnect;
  private TableId tableId;

  @Override
  public void put(Collection<SinkDataEntry> sinkDataEntries) {
    for (SinkDataEntry entry : sinkDataEntries) {
      Schema schema = entry.getSchema();
      Object[] payload = entry.getPayload();
      String result =
          Arrays.stream(entry.getPayload())
              .map(value -> "'" + value.toString() + "'")
              .collect(Collectors.joining(", "));
      String sql =
          String.format(
              "insert into %s (%s) values (%s) on duplicate key update %s=values(%s)",
              tableId.toString(),
              schema
                  .getFields()
                  .stream()
                  .map(field -> "`" + field.getName() + "`")
                  .collect(Collectors.joining(", ")),
              result,
              keyValue.getString(JdbcConfigKeys.ORDER_COLUMN),
              keyValue.getString(JdbcConfigKeys.ORDER_COLUMN));
      log.info("The sql is : {}", sql);
      jdbcConnect.execute(sql);
    }
  }

  @Override
  public void start(KeyValue config) {
    this.keyValue = config;
    JdbcConfig jdbcConfig =
        new JdbcConfig(
            config.getString(JdbcConfigKeys.JDBC_ENDPOINT),
            config.getInt(JdbcConfigKeys.JDBC_PORT),
            config.getString(JdbcConfigKeys.JDBC_DATABASE),
            config.getString(JdbcConfigKeys.JDBC_USERNAME),
            config.getString(JdbcConfigKeys.JDBC_PASSWORD));
    jdbcConnect = new MySQLConnect(jdbcConfig);
    jdbcConnect.init();
    tableId =
        new TableId(
            config.getString(JdbcConfigKeys.JDBC_DATABASE),
            config.getString(JdbcConfigKeys.TABLE_NAME));
  }

  @Override
  public void stop() {
    log.info("The sink task has stopped");
  }

  @Override
  public void pause() {
    log.info("The sink task has paused");
  }

  @Override
  public void resume() {
    log.info("The sink task has resumed");
  }
}
