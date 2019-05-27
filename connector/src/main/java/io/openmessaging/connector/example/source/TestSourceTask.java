package io.openmessaging.connector.example.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.connector.example.JdbcConfigKeys;
import io.openmessaging.connector.example.JdbcConnect;
import io.openmessaging.connector.example.JdbcConnect.JdbcConfig;
import io.openmessaging.connector.example.JdbcReader;
import io.openmessaging.connector.example.MySQLConnect;
import io.openmessaging.connector.example.MySQLSourceInfo;
import io.openmessaging.connector.example.TableId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
  private ScheduledExecutorService executorService;
  private List<SourceDataEntry> lists = new LinkedList<>();
  private JdbcReader jdbcReader;

  @Override
  public Collection<SourceDataEntry> poll() {
    List<SourceDataEntry> lists = new LinkedList<>(this.lists);
    this.lists.clear();
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
    doStart();
  }

  private void doStart() {
    executorService = Executors.newScheduledThreadPool(5);
    jdbcReader =
        new JdbcReader(columnNames, mySQLSourceInfo, jdbcConnect, schema, keyValue, lists::add);
    executorService.scheduleAtFixedRate(jdbcReader, 0, 10, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    executorService.shutdown();
    jdbcConnect.close();
    log.info("This task has stopped");
  }

  @Override
  public void pause() {
    jdbcReader.pause();
    log.info("This task has paused");
  }

  @Override
  public void resume() {
    jdbcReader.resume();
    log.info("This task has resumed");
  }
}
