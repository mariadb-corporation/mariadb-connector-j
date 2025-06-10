// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.function.Function;
import org.mariadb.jdbc.BasePreparedStatement;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.export.Prepare;

public interface Context {

  /**
   * return current thread id
   *
   * @return current server thread id
   */
  long getThreadId();

  /**
   * Indicate server connection Id (not truncated)
   *
   * @param connectionId connection id
   */
  void setThreadId(long connectionId);

  /**
   * Get server current auto_increment value
   *
   * @return server auto increment
   */
  Long getAutoIncrement();

  /**
   * Set server autoincrement value
   *
   * @param autoIncrement current server autoincrement value
   */
  void setAutoIncrement(long autoIncrement);

  /**
   * Set Maxscale version.
   *
   * @param maxscaleVersion maxscale version
   */
  void setMaxscaleVersion(String maxscaleVersion);

  /**
   * Retrieve maxscaleVersion or null if not the case
   *
   * @return maxscale version
   */
  String getMaxscaleVersion();

  /**
   * Set server redirection url
   *
   * @param redirectUrl redirect url
   */
  void setRedirectUrl(String redirectUrl);

  /**
   * get redirect Url if server indicate reconnection url
   *
   * @return null if no redirection required, value if so
   */
  String getRedirectUrl();

  /**
   * Get connection initial seed
   *
   * @return initial seed
   */
  byte[] getSeed();

  /**
   * has server capability
   *
   * @param flag capability to check
   * @return true if server has capability
   */
  boolean hasServerCapability(long flag);

  /**
   * has client capability
   *
   * @param flag capability to check
   * @return true if client has capability
   */
  boolean hasClientCapability(long flag);

  /**
   * Does server and client permit pipeline
   *
   * @return true if permitted
   */
  boolean permitPipeline();

  /**
   * Get server connection state
   *
   * @return server status
   */
  int getServerStatus();

  /**
   * Set server connection state
   *
   * @param serverStatus server status
   */
  void setServerStatus(int serverStatus);

  /**
   * Get current connection database
   *
   * @return database
   */
  String getDatabase();

  /**
   * set current database context
   *
   * @param database database
   */
  void setDatabase(String database);

  /**
   * Retrieve server version information
   *
   * @return server version
   */
  ServerVersion getVersion();

  /**
   * does protocol remove EOF in exchanges
   *
   * @return if EOF packet are deprecated
   */
  boolean isEofDeprecated();

  /**
   * Can server skip prepared statement metadata
   *
   * @return true if possible
   */
  boolean canSkipMeta();

  /**
   * Column decoder function
   *
   * @return Column decoder function
   */
  Function<ReadableByteBuf, ColumnDecoder> getColumnDecoderFunction();

  /**
   * has server warnings
   *
   * @return has warnings
   */
  int getWarning();

  /**
   * set server state warning number
   *
   * @param warning warning number
   */
  void setWarning(int warning);

  /**
   * Get connection exception factory
   *
   * @return exception factory
   */
  ExceptionFactory getExceptionFactory();

  /**
   * Get connection configuration
   *
   * @return configuration
   */
  Configuration getConf();

  /**
   * Can rely on transaction_isolation or keep using deprecated tx_isolation variable
   *
   * @return true if you can use transaction_isolation
   */
  boolean canUseTransactionIsolation();

  /**
   * Get connection transaction isolation level
   *
   * @return connection transaction isolation level
   */
  Integer getTransactionIsolationLevel();

  /**
   * Set current connection transaction isolation level
   *
   * @param transactionIsolationLevel new connection transaction isolation level
   */
  void setTransactionIsolationLevel(Integer transactionIsolationLevel);

  /**
   * Return cached prepare if key match
   *
   * @param sql sql command
   * @param preparedStatement current statement
   * @return Prepare if found, null if not
   */
  Prepare getPrepareCacheCmd(String sql, BasePreparedStatement preparedStatement);

  /**
   * Put prepare result in cache
   *
   * @param sql sql command
   * @param result prepare result
   * @param preparedStatement current statement
   * @return Prepare if was already cached
   */
  Prepare putPrepareCacheCmd(String sql, Prepare result, BasePreparedStatement preparedStatement);

  /** Reset prepare cache (after a failover) */
  void resetPrepareCache();

  /**
   * return connection current state change flag
   *
   * @return connection current state change flag
   */
  int getStateFlag();

  /** reset connection state change flag */
  void resetStateFlag();

  /**
   * Indicate connection state (for pooling)
   *
   * @param state indicate that some connection state has changed
   */
  void addStateFlag(int state);

  /**
   * Indicate the number of connection on this server
   *
   * @param threadsConnected number of connected threads
   */
  void setTreadsConnected(long threadsConnected);

  /**
   * Retrieve current charset if session state get it
   *
   * @return current charset
   */
  String getCharset();

  /**
   * Indicate server charset change
   *
   * @param charset server charset
   */
  void setCharset(String charset);

  /**
   * Get current connection timezone
   *
   * @return connection timezone
   */
  TimeZone getConnectionTimeZone();

  /**
   * Set current connection timezone
   *
   * @param connectionTimeZone connection timezone
   */
  void setConnectionTimeZone(TimeZone connectionTimeZone);

  /**
   * Get calendar depending on configuration
   *
   * @return calendar
   */
  Calendar getDefaultCalendar();
}
