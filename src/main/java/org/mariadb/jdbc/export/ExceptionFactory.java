// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.export;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.MariaDbPoolConnection;
import org.mariadb.jdbc.client.Completion;
import org.mariadb.jdbc.message.server.OkPacket;
import org.mariadb.jdbc.util.ThreadUtils;

/**
 * Exception factory. This permit common error logging, with thread id, dump query, and specific
 * dead-lock additional information
 */
public class ExceptionFactory {

  private static final Set<Integer> LOCK_DEADLOCK_ERROR_CODES =
      new HashSet<>(Arrays.asList(1205, 1213, 1614));
  private final Configuration conf;
  private final HostAddress hostAddress;
  private Connection connection;
  private MariaDbPoolConnection poolConnection;
  private long threadId;
  private Statement statement;

  /**
   * Connection Exception factory constructor
   *
   * @param conf configuration
   * @param hostAddress current host
   */
  public ExceptionFactory(Configuration conf, HostAddress hostAddress) {
    this.conf = conf;
    this.hostAddress = hostAddress;
  }

  private ExceptionFactory(
      Connection connection,
      MariaDbPoolConnection poolConnection,
      Configuration conf,
      HostAddress hostAddress,
      long threadId,
      Statement statement) {
    this.connection = connection;
    this.poolConnection = poolConnection;
    this.conf = conf;
    this.hostAddress = hostAddress;
    this.threadId = threadId;
    this.statement = statement;
  }

  private static String buildMsgText(
      String initialMessage,
      long threadId,
      Configuration conf,
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

    if (conf.includeInnodbStatusInDeadlockExceptions()
        && LOCK_DEADLOCK_ERROR_CODES.contains(errorCode)
        && connection != null) {
      Statement stmt = connection.createStatement();
      try {
        ResultSet rs = stmt.executeQuery("SHOW ENGINE INNODB STATUS");
        rs.next();
        msg.append("\ndeadlock information: ").append(rs.getString(3));
      } catch (SQLException sqle) {
        // eat
      }
    }

    if (conf.includeThreadDumpInDeadlockExceptions()
        && LOCK_DEADLOCK_ERROR_CODES.contains(errorCode)) {
      msg.append("\nthread name: ").append(Thread.currentThread().getName());
      msg.append("\ncurrent threads: ");
      Thread.getAllStackTraces()
          .forEach(
              (thread, traces) -> {
                msg.append("\n  name:\"")
                    .append(thread.getName())
                    .append("\" pid:")
                    .append(ThreadUtils.getId(thread))
                    .append(" status:")
                    .append(thread.getState());
                for (StackTraceElement trace : traces) {
                  msg.append("\n    ").append(trace);
                }
              });
    }

    return msg.toString();
  }

  /**
   * Set connection
   *
   * @param oldExceptionFactory previous connection exception factory
   */
  public void setConnection(ExceptionFactory oldExceptionFactory) {
    this.connection = oldExceptionFactory.connection;
  }

  /**
   * Set connection to factory
   *
   * @param connection connection
   * @return this {@link ExceptionFactory}
   */
  public ExceptionFactory setConnection(Connection connection) {
    this.connection = connection;
    return this;
  }

  /**
   * Set pool connection to factory
   *
   * @param internalPoolConnection internal pool connection
   * @return this {@link ExceptionFactory}
   */
  public ExceptionFactory setPoolConnection(MariaDbPoolConnection internalPoolConnection) {
    this.poolConnection = internalPoolConnection;
    return this;
  }

  /**
   * Set connection thread id
   *
   * @param threadId connection thread id
   */
  public void setThreadId(long threadId) {
    this.threadId = threadId;
  }

  /**
   * Create a BatchUpdateException, filling successful updates
   *
   * @param res completion list
   * @param length expected size
   * @param sqle exception
   * @return BatchUpdateException object
   */
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
    return new BatchUpdateException(
        sqle.getMessage(), sqle.getSQLState(), sqle.getErrorCode(), updateCounts, sqle);
  }

  /**
   * Create a BatchUpdateException, filling successful updates
   *
   * @param res completion list
   * @param length expected length
   * @param responseMsg successful response
   * @param sqle exception
   * @return BatchUpdateException object
   */
  public BatchUpdateException createBatchUpdate(
      List<Completion> res, int length, int[] responseMsg, SQLException sqle) {
    int[] updateCounts = new int[length];
    for (int i = 0; i < length; i++) {
      if (i >= responseMsg.length) {
        Arrays.fill(updateCounts, i, length, Statement.EXECUTE_FAILED);
        break;
      }
      int MsgResponseNo = responseMsg[i];
      if (MsgResponseNo < 1) {
        updateCounts[0] = Statement.EXECUTE_FAILED;
        return new BatchUpdateException(updateCounts, sqle);
      } else if (MsgResponseNo == 1) {
        if (i >= res.size() || res.get(i) == null) {
          updateCounts[i] = Statement.EXECUTE_FAILED;
          continue;
        }
        if (res.get(i) instanceof OkPacket) {
          updateCounts[i] = (int) ((OkPacket) res.get(i)).getAffectedRows();
          continue;
        }
        updateCounts[i] = Statement.SUCCESS_NO_INFO;
      } else {
        // unknown.
        updateCounts[i] = Statement.SUCCESS_NO_INFO;
      }
    }
    return new BatchUpdateException(
        sqle.getMessage(), sqle.getSQLState(), sqle.getErrorCode(), updateCounts, sqle);
  }

  /**
   * Construct an Exception factory from this + adding current statement
   *
   * @param statement current statement
   * @return new Exception factory
   */
  public ExceptionFactory of(Statement statement) {
    return new ExceptionFactory(
        this.connection,
        this.poolConnection,
        this.conf,
        this.hostAddress,
        this.threadId,
        statement);
  }

  /**
   * Construct an Exception factory from this + adding current SQL
   *
   * @param sql current sql command
   * @return new Exception factory
   */
  public ExceptionFactory withSql(String sql) {
    return new SqlExceptionFactory(
        this.connection,
        this.poolConnection,
        this.conf,
        this.hostAddress,
        this.threadId,
        statement,
        sql);
  }

  private SQLException createException(
      String initialMessage, String sqlState, int errorCode, Exception cause, boolean isPrepare) {

    String msg = buildMsgText(initialMessage, threadId, conf, getSql(), errorCode, connection);

    if ("70100".equals(sqlState)) { // ER_QUERY_INTERRUPTED
      return new SQLTimeoutException(msg, sqlState, errorCode);
    }
    // 4166 : mariadb load data infile disable
    // 1148 : 10.2 mariadb load data infile disable
    // 3948 : mysql load data infile disable
    if ((errorCode == 4166 || errorCode == 3948 || errorCode == 1148) && !conf.allowLocalInfile()) {
      return new SQLException(
          "Local infile is disabled by connector. Enable `allowLocalInfile` to allow local infile"
              + " commands",
          sqlState,
          errorCode,
          cause);
    }

    if (errorCode == 1264 || errorCode == 1265 || errorCode == 1292 || errorCode == 1406) {
      return new MariaDbDataTruncation(msg, sqlState, errorCode, cause);
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
        if (isPrepare) {
          returnEx = new SQLPrepareException(msg, sqlState, errorCode, cause);
        } else {
          returnEx = new SQLSyntaxErrorException(msg, sqlState, errorCode, cause);
        }
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
      case "HY":
        if (isPrepare) {
          returnEx = new SQLPrepareException(msg, sqlState, errorCode, cause);
        } else {
          returnEx = new SQLException(msg, sqlState, errorCode, cause);
        }
        break;
      default:
        returnEx = new SQLTransientConnectionException(msg, sqlState, errorCode, cause);
        break;
    }

    if (poolConnection != null) {
      if (statement != null && statement instanceof PreparedStatement) {
        poolConnection.fireStatementErrorOccurred((PreparedStatement) statement, returnEx);
      }
      if (returnEx instanceof SQLNonTransientConnectionException
          || returnEx instanceof SQLTransientConnectionException) {
        poolConnection.fireConnectionErrorOccurred(returnEx);
      }
    }

    return returnEx;
  }

  /**
   * fast creation of SQLFeatureNotSupportedException exception
   *
   * @param message error message
   * @return exception to be thrown
   */
  public SQLException notSupported(String message) {
    return createException(message, "0A000", -1, null, false);
  }

  /**
   * Creation of an exception
   *
   * @param message error message
   * @return exception to be thrown
   */
  public SQLException create(String message) {
    return createException(message, "42000", -1, null, false);
  }

  /**
   * Creation of an exception
   *
   * @param message error message
   * @param sqlState sql state
   * @return exception to be thrown
   */
  public SQLException create(String message, String sqlState) {
    return createException(message, sqlState, -1, null, false);
  }

  /**
   * Creation of an exception
   *
   * @param message error message
   * @param sqlState sql state
   * @param cause initial exception
   * @return exception to be thrown
   */
  public SQLException create(String message, String sqlState, Exception cause) {
    return createException(message, sqlState, -1, cause, false);
  }

  /**
   * Creation of an exception
   *
   * @param message error message
   * @param sqlState sql state
   * @param errorCode error code
   * @return exception to be thrown
   */
  public SQLException create(String message, String sqlState, int errorCode) {
    return createException(message, sqlState, errorCode, null, false);
  }

  /**
   * Creation of an exception
   *
   * @param message error message
   * @param sqlState sql state
   * @param errorCode error code
   * @return exception to be thrown
   */
  public SQLException create(String message, String sqlState, int errorCode, boolean isPrepare) {
    return createException(message, sqlState, errorCode, null, isPrepare);
  }

  /**
   * get SQL command
   *
   * @return sql command
   */
  public String getSql() {
    return null;
  }

  /** Exception with SQL command */
  public class SqlExceptionFactory extends ExceptionFactory {
    private final String sql;

    /**
     * Constructor of Exception factory with SQL
     *
     * @param connection connection
     * @param poolConnection pool connection
     * @param conf configuration
     * @param hostAddress host
     * @param threadId connection thread id
     * @param statement statement
     * @param sql sql
     */
    public SqlExceptionFactory(
        Connection connection,
        MariaDbPoolConnection poolConnection,
        Configuration conf,
        HostAddress hostAddress,
        long threadId,
        Statement statement,
        String sql) {
      super(connection, poolConnection, conf, hostAddress, threadId, statement);
      this.sql = sql;
    }

    @Override
    public String getSql() {
      return sql;
    }
  }
}
