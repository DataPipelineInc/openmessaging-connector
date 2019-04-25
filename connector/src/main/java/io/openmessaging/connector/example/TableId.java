package io.openmessaging.connector.example;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class TableId {
  private String database;
  private String tablename;

  public TableId(String database, String tablename) {
    this.database = database;
    this.tablename = tablename;
  }

  public String getDatabase() {
    return database;
  }

  public String getTablename() {
    return tablename;
  }

  @Override
  public String toString() {
    return String.format("`%s`.`%s`", database, tablename);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    TableId that = (TableId) obj;

    return new EqualsBuilder()
        .append(database, that.database)
        .append(tablename, that.tablename)
        .build();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(database).append(tablename).build();
  }
}
