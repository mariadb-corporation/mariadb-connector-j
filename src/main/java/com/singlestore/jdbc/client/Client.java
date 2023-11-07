// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client;

import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.export.ExceptionFactory;
import com.singlestore.jdbc.export.Prepare;
import com.singlestore.jdbc.message.ClientMessage;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executor;

public interface Client extends AutoCloseable {

  /**
   * Send client message and read result
   *
   * @param message client message
   * @param canRedo can client message be redone in case of failover
   * @return results
   * @throws SQLException if execution fails
   */
  List<Completion> execute(ClientMessage message, boolean canRedo) throws SQLException;

  /**
   * Send client message and read result
   *
   * @param message client message
   * @param stmt statement
   * @param canRedo can client message be redone in case of failover
   * @return results
   * @throws SQLException if execution fails
   */
  List<Completion> execute(ClientMessage message, Statement stmt, boolean canRedo)
      throws SQLException;

  /**
   * Send client message and read result
   *
   * @param message client message
   * @param stmt statement
   * @param fetchSize fetch size
   * @param maxRows maximum number of rows. 0 = all
   * @param resultSetConcurrency concurrency
   * @param resultSetType result-set type
   * @param closeOnCompletion close statement on completion
   * @param canRedo can client message be redone in case of failover
   * @return results
   * @throws SQLException if any error occurs
   */
  List<Completion> execute(
      ClientMessage message,
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      boolean canRedo)
      throws SQLException;

  /**
   * Send client messages pipelining and read result
   *
   * @param messages client message
   * @param stmt statement
   * @param fetchSize fetch size
   * @param maxRows maximum number of rows. 0 = all
   * @param resultSetConcurrency concurrency
   * @param resultSetType result-set type
   * @param closeOnCompletion close statement on completion
   * @param canRedo can client message be redone in case of failover
   * @return results
   * @throws SQLException if any error occurs
   */
  List<Completion> executePipeline(
      ClientMessage[] messages,
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      boolean canRedo)
      throws SQLException;

  /**
   * Read results
   *
   * @param completions List that will have the new results
   * @param fetchSize fetch size
   * @param maxRows maximum number of rows. 0 = all
   * @param resultSetConcurrency concurrency
   * @param resultSetType result-set type
   * @param closeOnCompletion close statement on completion
   * @throws SQLException if any error occurs
   */
  void readStreamingResults(
      List<Completion> completions,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException;

  /**
   * Close prepare command
   *
   * @param prepare prepare command
   * @throws SQLException if any error occurs
   */
  void closePrepare(Prepare prepare) throws SQLException;

  /**
   * Abort current connection
   *
   * @param executor executor
   * @throws SQLException if any error occurs
   */
  void abort(Executor executor) throws SQLException;

  /**
   * Close client
   *
   * @throws SQLException if any error occurs
   */
  void close() throws SQLException;

  /**
   * Switch to a writer/read-only connection, no effet on mono-connection
   *
   * @param readOnly must use read-only connection
   * @throws SQLException if any error occurs
   */
  void setReadOnly(boolean readOnly) throws SQLException;

  /**
   * get socket timeout
   *
   * @return socket timeout
   */
  int getSocketTimeout();

  /**
   * Set socket timeout
   *
   * @param milliseconds timeout
   * @throws SQLException if any error occurs
   */
  void setSocketTimeout(int milliseconds) throws SQLException;

  /**
   * Is client closed
   *
   * @return close flag
   */
  boolean isClosed();

  /** Reset connection */
  void reset();

  /**
   * is current client writer or read-only
   *
   * @return is primary
   */
  boolean isPrimary();

  /**
   * Get connection context
   *
   * @return connection context
   */
  Context getContext();

  /**
   * Get connection exception factory
   *
   * @return connection exception factory
   */
  ExceptionFactory getExceptionFactory();

  /**
   * Get connection host
   *
   * @return connection host
   */
  HostAddress getHostAddress();

  /**
   * Get aggregator id
   *
   * @return aggregator id
   */
  BigInteger getAggregatorId();
}
