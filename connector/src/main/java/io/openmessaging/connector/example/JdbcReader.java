package io.openmessaging.connector.example;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.DataEntryBuilder;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.runtime.TaskConfig;
import io.openmessaging.connector.runtime.rest.error.ConnectException;
import io.openmessaging.connector.runtime.utils.ConvertUtils;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcReader implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(JdbcReader.class);
  private List<String> columnNames;
  private MySQLSourceInfo mySQLSourceInfo;
  private JdbcConnect jdbcConnect;
  private boolean pause = false;
  private Schema schema;
  private KeyValue keyValue;
  private SourceDataEntryConsumer consumer;

  public JdbcReader(
      List<String> columnNames,
      MySQLSourceInfo mySQLSourceInfo,
      JdbcConnect jdbcConnect,
      Schema schema,
      KeyValue keyValue,
      SourceDataEntryConsumer consumer) {
    this.columnNames = columnNames;
    this.mySQLSourceInfo = mySQLSourceInfo;
    this.jdbcConnect = jdbcConnect;
    this.schema = schema;
    this.keyValue = keyValue;
    this.consumer = consumer;
  }

  @Override
  public void run() {
    synchronized (this) {
      while (pause) {
        try {
          wait();
        } catch (InterruptedException ignore) {
        }
      }
    }

    String sql = mySQLSourceInfo.buildSql();
    log.info("The sql is : {}", sql);
    jdbcConnect.executeQuery(
        sql,
        resultSet -> {
          try {
            while (resultSet.next()) {
              DataEntryBuilder builder = new DataEntryBuilder(schema);
              builder.queue(keyValue.getString(TaskConfig.TASK_TOPICS_CONFIG));
              for (String columnName : columnNames) {
                String value = resultSet.getString(columnName);
                builder.putFiled(columnName, value);
                if (columnName.equals(mySQLSourceInfo.getOrderColumn())) {
                  mySQLSourceInfo.updateSelectCondition(
                      String.format(
                          "`%s`.`%s` > %s",
                          keyValue.getString(JdbcConfigKeys.TABLE_NAME),
                          keyValue.getString(JdbcConfigKeys.ORDER_COLUMN),
                          value));
                }
              }
              Map<String, Object> position = new HashMap<>();
              position.put("selectCondition", mySQLSourceInfo.getSelectCondition());
              consumer.accept(
                  builder.buildSourceDataEntry(
                      ByteBuffer.wrap(ConvertUtils.getBytesfromObject("partition01")),
                      ByteBuffer.wrap(ConvertUtils.getBytesfromObject(position))));
            }
          } catch (SQLException e) {
            throw new ConnectException(e);
          }
        });
  }

  public void pause() {
    synchronized (this) {
      this.pause = true;
      notifyAll();
    }
  }

  public void resume() {
    synchronized (this) {
      this.pause = false;
      notifyAll();
    }
  }

  public interface SourceDataEntryConsumer {
    void accept(SourceDataEntry sourceDataEntry);
  }
}
