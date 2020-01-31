package org.mariadb.jdbc.internal.util.exceptions;

import java.sql.SQLException;

public class MariaDbSqlException extends SQLException {

  private String sql;
  private String threadName = null;
  private String deadLockInfo = null;

  public MariaDbSqlException(String reason, String sql, Throwable cause) {
    super(reason, cause);
    this.sql = sql;
  }

  public MariaDbSqlException(String reason, String sql, String sqlState, Throwable cause) {
    super(reason, sqlState, cause);
    this.sql = sql;
  }

  public MariaDbSqlException(
      String reason, String sql, String sqlState, int vendorCode, Throwable cause) {
    super(reason, sqlState, vendorCode, cause);
    this.sql = sql;
  }

  public static MariaDbSqlException of(SQLException cause, String sql) {
    return new MariaDbSqlException(
        cause.getMessage(), sql, cause.getSQLState(), cause.getErrorCode(), cause);
  }

  public MariaDbSqlException withThreadName(String threadName) {
    this.threadName = threadName;
    return this;
  }

  public MariaDbSqlException withDeadLockInfo(String deadLockInfo) {
    this.deadLockInfo = deadLockInfo;
    return this;
  }

  public String getSql() {
    return sql;
  }

  public String getThreadName() {
    return threadName;
  }

  public String getDeadLockInfo() {
    return deadLockInfo;
  }
}
