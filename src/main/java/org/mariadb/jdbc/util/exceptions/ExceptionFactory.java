package org.mariadb.jdbc.util.exceptions;

import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.sql.ConnectionEvent;
import javax.sql.StatementEvent;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.message.server.Completion;
import org.mariadb.jdbc.message.server.OkPacket;

public class ExceptionFactory {

  private static final Set<Integer> LOCK_DEADLOCK_ERROR_CODES =
      new HashSet<>(Arrays.asList(1205, 1213, 1614));
  private final Configuration conf;
  private final HostAddress hostAddress;
  private Connection connection;
  private long threadId;
  private Statement statement;

  public ExceptionFactory(Configuration conf, HostAddress hostAddress) {
    this.conf = conf;
    this.hostAddress = hostAddress;
  }

  private ExceptionFactory(
      Connection connection,
      Configuration conf,
      HostAddress hostAddress,
      long threadId,
      Statement statement) {
    this.connection = connection;
    this.conf = conf;
    this.hostAddress = hostAddress;
    this.threadId = threadId;
    this.statement = statement;
  }

  private static String buildMsgText(
      String initialMessage,
      long threadId,
      Configuration conf,
      Exception cause,
      String sql,
      int errorCode,
      Connection connection) {

    StringBuilder msg = new StringBuilder();

    if (threadId != 0L) {
      msg.append("(conn=").append(threadId).append(") ");
    }
    msg.append(initialMessage);

    if (conf.dumpQueriesOnException() && sql != null) {
      if (conf.maxQuerySizeToLog() != 0 && sql.length() > conf.maxQuerySizeToLog() - 3) {
        msg.append("\nQuery is: ").append(sql, 0, conf.maxQuerySizeToLog() - 3).append("...");
      } else {
        msg.append("\nQuery is: ").append(sql);
      }
    }

    if (conf.includeInnodbStatusInDeadlockExceptions()) {
      if (LOCK_DEADLOCK_ERROR_CODES.contains(errorCode) && connection != null) {
        Statement stmt = connection.createStatement();
        try {
          ResultSet rs = stmt.executeQuery("SHOW ENGINE INNODB STATUS");
          if (rs.next()) {
            msg.append("\ndeadlock information: ").append(rs.getString(3));
          }
        } catch (SQLException sqle) {
          // eat
        }
      }
    }

    if (conf.includeThreadDumpInDeadlockExceptions()) {
      msg.append("\nthread name: ").append(Thread.currentThread().getName());
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

  public ExceptionFactory setConnection(ExceptionFactory oldExceptionFactory) {
    this.connection = oldExceptionFactory.connection;
    return this;
  }

  public ExceptionFactory setConnection(Connection connection) {
    this.connection = connection;
    return this;
  }

  public void setThreadId(long threadId) {
    this.threadId = threadId;
  }

  public BatchUpdateException createBatchUpdate(
      List<Completion> res, int length, SQLException sqle) {
    int[] updateCounts = new int[length];
    for (int i = 0; i < length; i++) {
      if (i < res.size()) {
        if (res.get(i) instanceof OkPacket) {
          updateCounts[i] = (int) ((OkPacket) res.get(i)).getAffectedRows();
        } else {
          updateCounts[i] = org.mariadb.jdbc.Statement.SUCCESS_NO_INFO;
        }
      } else {
        updateCounts[i] = org.mariadb.jdbc.Statement.EXECUTE_FAILED;
      }
    }
    return new BatchUpdateException(updateCounts, sqle);
  }

  public BatchUpdateException createBatchUpdate(
      List<Completion> res, int length, int[] responseMsg, SQLException sqle) {
    int[] updateCounts = new int[length];

    int responseIncrement = 0;
    for (int i = 0; i < length; i++) {
      if (i >= responseMsg.length) {
        Arrays.fill(updateCounts, i, length, Statement.EXECUTE_FAILED);
        break;
      }
      int MsgResponseNo = responseMsg[i];
      if (MsgResponseNo < 1) {
        updateCounts[responseIncrement++] = Statement.EXECUTE_FAILED;
        return new BatchUpdateException(updateCounts, sqle);
      } else if (MsgResponseNo == 1 && res.size() > i && res.get(i) instanceof OkPacket) {
        updateCounts[i] = (int) ((OkPacket) res.get(i)).getAffectedRows();
      } else {
        // unknown.
        updateCounts[i] = Statement.SUCCESS_NO_INFO;
      }
    }
    return new BatchUpdateException(updateCounts, sqle);
  }

  public ExceptionFactory of(Statement statement) {
    return new ExceptionFactory(
        this.connection, this.conf, this.hostAddress, this.threadId, statement);
  }

  public ExceptionFactory withSql(String sql) {
    return new SqlExceptionFactory(
        this.connection, this.conf, this.hostAddress, this.threadId, statement, sql);
  }

  private SQLException createException(
      String initialMessage, String sqlState, int errorCode, Exception cause) {

    String msg =
        buildMsgText(initialMessage, threadId, conf, cause, getSql(), errorCode, connection);

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

    if (statement != null && statement instanceof PreparedStatement) {
      StatementEvent event =
          new StatementEvent(connection, (PreparedStatement) statement, returnEx);
      connection.fireStatementErrorOccurred(event);
    }

    if (connection != null
        && (returnEx instanceof SQLNonTransientConnectionException
            || returnEx instanceof SQLTransientConnectionException)) {
      ConnectionEvent event = new ConnectionEvent(connection, returnEx);
      connection.fireConnectionErrorOccurred(event);
    }

    return returnEx;
  }

  public SQLException notSupported(String message) {
    return createException(message, "0A000", -1, null);
  }

  public SQLException create(String message) {
    return createException(message, "42000", -1, null);
  }

  public SQLException create(String message, String sqlState) {
    return createException(message, sqlState, -1, null);
  }

  public SQLException create(String message, String sqlState, Exception cause) {
    return createException(message, sqlState, -1, cause);
  }

  public SQLException create(String message, String sqlState, int errorCode) {
    return createException(message, sqlState, errorCode, null);
  }

  public String getSql() {
    return null;
  }

  public class SqlExceptionFactory extends ExceptionFactory {
    private final String sql;

    public SqlExceptionFactory(
        Connection connection,
        Configuration conf,
        HostAddress hostAddress,
        long threadId,
        Statement statement,
        String sql) {
      super(connection, conf, hostAddress, threadId, statement);
      this.sql = sql;
    }

    public String getSql() {
      return sql;
    }
  }
}
