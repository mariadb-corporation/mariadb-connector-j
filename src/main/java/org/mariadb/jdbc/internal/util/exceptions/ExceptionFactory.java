package org.mariadb.jdbc.internal.util.exceptions;

import java.sql.*;
import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.util.Options;

public final class ExceptionFactory {

  public static final ExceptionFactory INSTANCE = new ExceptionFactory(-1L, null);

  private final long threadId;
  private final Options options;

  private MariaDbConnection connection;
  private Statement statement;

  public ExceptionFactory(
      long threadId, Options options, MariaDbConnection connection, Statement statement) {
    this.threadId = threadId;
    this.options = options;
    this.connection = connection;
    this.statement = statement;
  }

  private ExceptionFactory(long threadId, Options options) {
    this.threadId = threadId;
    this.options = options;
  }

  public static ExceptionFactory of(long threadId, Options options) {
    return new ExceptionFactory(threadId, options);
  }

  private static SQLException createException(
      String initialMessage,
      String sqlState,
      int errorCode,
      long threadId,
      Options options,
      MariaDbConnection connection,
      Statement statement,
      Exception cause) {

    String msg = buildMsgText(initialMessage, threadId, options, cause);

    if ("70100".equals(sqlState)) { // ER_QUERY_INTERRUPTED
      return new SQLTimeoutException(msg, sqlState, errorCode);
    }

    SQLException returnEx;
    String sqlClass = sqlState == null ? "42" : sqlState.substring(0, 2);
    switch (sqlClass) {
      case "0A":
        returnEx = new SQLFeatureNotSupportedException(msg, sqlState, errorCode, cause);
        break;
      case "22":
      case "26":
      case "2F":
      case "20":
      case "42":
      case "XA":
        returnEx = new SQLSyntaxErrorException(msg, sqlState, errorCode, cause);
        break;
      case "25":
      case "28":
        returnEx = new SQLInvalidAuthorizationSpecException(msg, sqlState, errorCode, cause);
        break;
      case "21":
      case "23":
        returnEx = new SQLIntegrityConstraintViolationException(msg, sqlState, errorCode, cause);
        break;
      case "08":
        returnEx = new SQLNonTransientConnectionException(msg, sqlState, errorCode, cause);
        break;
      case "40":
        returnEx = new SQLTransactionRollbackException(msg, sqlState, errorCode, cause);
        break;
      default:
        returnEx = new SQLTransientConnectionException(msg, sqlState, errorCode, cause);
        break;
    }

    if (connection != null && connection.pooledConnection != null) {
      connection.pooledConnection.fireStatementErrorOccurred(statement, returnEx);
      if (returnEx instanceof SQLNonTransientConnectionException) {
        connection.pooledConnection.fireConnectionErrorOccurred(returnEx);
      }
    }
    return returnEx;
  }

  private static String buildMsgText(
      String initialMessage, long threadId, Options options, Exception cause) {

    StringBuilder msg = new StringBuilder();
    String deadLockException = null;
    String threadName = null;

    if (threadId != -1L) {
      msg.append("(conn=").append(threadId).append(") ").append(initialMessage);
    } else {
      msg.append(initialMessage);
    }

    if (cause instanceof MariaDbSqlException) {
      MariaDbSqlException exception = ((MariaDbSqlException) cause);
      String sql = exception.getSql();
      if (options.dumpQueriesOnException && sql != null) {
        if (options != null
            && options.maxQuerySizeToLog != 0
            && sql.length() > options.maxQuerySizeToLog - 3) {
          msg.append("\nQuery is: ").append(sql, 0, options.maxQuerySizeToLog - 3).append("...");
        } else {
          msg.append("\nQuery is: ").append(sql);
        }
      }
      deadLockException = exception.getDeadLockInfo();
      threadName = exception.getThreadName();
    }

    if (options != null
        && options.includeInnodbStatusInDeadlockExceptions
        && deadLockException != null) {
      msg.append("\ndeadlock information: ").append(deadLockException);
    }

    if (options != null
        && deadLockException != null
        && options.includeThreadDumpInDeadlockExceptions) {
      if (threadName != null) {
        msg.append("\nthread name: ").append(threadName);
      }
      msg.append("\ncurrent threads: ");
      Thread.getAllStackTraces()
          .forEach(
              (thread, traces) -> {
                msg.append("\n  name:\"")
                    .append(thread.getName())
                    .append("\" pid:")
                    .append(thread.getId())
                    .append(" status:")
                    .append(thread.getState());
                for (int i = 0; i < traces.length; i++) {
                  msg.append("\n    ").append(traces[i]);
                }
              });
    }

    return msg.toString();
  }

  public ExceptionFactory raiseStatementError(MariaDbConnection connection, Statement stmt) {
    return new ExceptionFactory(threadId, options, connection, stmt);
  }

  public SQLException create(SQLException cause) {

    return createException(
        cause.getMessage().contains("\n")
            ? cause.getMessage().substring(0, cause.getMessage().indexOf("\n"))
            : cause.getMessage(),
        cause.getSQLState(),
        cause.getErrorCode(),
        threadId,
        options,
        connection,
        statement,
        cause);
  }

  public SQLException notSupported(String message) {
    return createException(message, "0A000", -1, threadId, options, connection, statement, null);
  }

  public SQLException create(String message) {
    return createException(message, "42000", -1, threadId, options, connection, statement, null);
  }

  public SQLException create(String message, Exception cause) {
    return createException(message, "42000", -1, threadId, options, connection, statement, cause);
  }

  public SQLException create(String message, String sqlState) {
    return createException(message, sqlState, -1, threadId, options, connection, statement, null);
  }

  public SQLException create(String message, String sqlState, Exception cause) {
    return createException(message, sqlState, -1, threadId, options, connection, statement, cause);
  }

  public SQLException create(String message, String sqlState, int errorCode) {
    return createException(
        message, sqlState, errorCode, threadId, options, connection, statement, null);
  }

  public SQLException create(String message, String sqlState, int errorCode, Exception cause) {
    return createException(
        message, sqlState, errorCode, threadId, options, connection, statement, cause);
  }

  public long getThreadId() {
    return threadId;
  }

  public Options getOptions() {
    return options;
  }

  @Override
  public String toString() {
    return "ExceptionFactory{threadId=" + threadId + '}';
  }
}
