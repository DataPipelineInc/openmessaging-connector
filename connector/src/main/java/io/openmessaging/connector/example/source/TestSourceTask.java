package io.openmessaging.connector.example.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.DataEntryBuilder;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.connector.example.JdbcConfigKeys;
import io.openmessaging.connector.example.JdbcConnect;
import io.openmessaging.connector.example.JdbcConnect.JdbcConfig;
import io.openmessaging.connector.example.MySQLConnect;
import io.openmessaging.connector.example.MySQLSourceInfo;
import io.openmessaging.connector.example.TableId;
import io.openmessaging.connector.runtime.rest.error.ConnectException;
import io.openmessaging.connector.runtime.utils.ConvertUtils;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSourceTask extends SourceTask {
  private static final Logger log = LoggerFactory.getLogger(TestSourceTask.class);
  private JdbcConnect jdbcConnect;
  private MySQLSourceInfo mySQLSourceInfo;
  private int i = 0;
  private KeyValue keyValue;
  private List<String> columnNames;
  private Schema schema;

  @Override
  public Collection<SourceDataEntry> poll() {
    List<SourceDataEntry> lists = new ArrayList<>();
    try {
      String sql = mySQLSourceInfo.buildSql();
      ResultSet resultSet = jdbcConnect.executeQuery(sql);
      while (resultSet.next()) {
        DataEntryBuilder builder = new DataEntryBuilder(schema);
        builder.queue("ab2");
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
        lists.add(
            builder.buildSourceDataEntry(
                ByteBuffer.wrap(ConvertUtils.getBytesfromObject("partition01")),
                ByteBuffer.wrap(ConvertUtils.getBytesfromObject(position))));
      }

    } catch (SQLException e) {
      throw new ConnectException(e);
    }
    return lists;
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
    mySQLSourceInfo =
        new MySQLSourceInfo(
            new TableId(
                config.getString(JdbcConfigKeys.JDBC_DATABASE),
                config.getString(JdbcConfigKeys.TABLE_NAME)),
            config.getString(JdbcConfigKeys.ORDER_COLUMN));
    this.columnNames = mySQLSourceInfo.getColumnNames(jdbcConnect);
    schema = new Schema();
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < columnNames.size(); i++) {
      fields.add(new Field(i, columnNames.get(i), FieldType.STRING));
    }
    schema.setFields(fields);
  }

  @Override
  public void stop() {
    jdbcConnect.close();
    log.info("This task has stopped");
  }

  @Override
  public void pause() {
    log.info("This task has paused");
  }

  @Override
  public void resume() {
    log.info("This task has resumed");
  }
}
