package io.openmessaging.connector.example;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;

public class MySQLSourceInfo {
  private TableId tableId;
  private AtomicReference<String> selectCondition;
  private String orderColumn;

  public MySQLSourceInfo(TableId tableId, String orderColumn) {
    this.tableId = tableId;
    this.orderColumn = orderColumn;
  }

  public TableId getTableId() {
    return tableId;
  }

  public List<String> getColumnNames(JdbcConnect connect) {
    return connect.readColumns(tableId);
  }

  public void updateSelectCondition(String newSqlCondition) {
    this.selectCondition.set(newSqlCondition);
  }

  public String getSelectCondition() {
    return selectCondition.get();
  }

  public String getOrderColumn() {
    return orderColumn;
  }

  public String buildSql() {
    StringBuilder sqlBuilder = new StringBuilder("select * from " + this.tableId.toString());
    String condition = selectCondition.get();
    if (StringUtils.isNoneBlank(condition)) {
      sqlBuilder.append(" WHERE ").append(condition);
    }
    sqlBuilder.append("order by `" + this.orderColumn + "` ASC");
    return sqlBuilder.toString();
  }
}
