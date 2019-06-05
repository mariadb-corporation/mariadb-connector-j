/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.BatchUpdateException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.mariadb.jdbc.internal.com.read.dao.Results;
import org.mariadb.jdbc.internal.com.read.resultset.SelectResultSet;
import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.util.Utils;
import org.mariadb.jdbc.internal.util.exceptions.ExceptionMapper;
import org.mariadb.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder;


public class MariaDbStatement implements Statement, Cloneable {

  //timeout scheduler
  private static final ScheduledExecutorService timeoutScheduler = SchedulerServiceProviderHolder
      .getTimeoutScheduler();
  private static final Logger logger = LoggerFactory.getLogger(MariaDbStatement.class);
  protected final ReentrantLock lock;
  protected final int resultSetScrollType;
  protected final int resultSetConcurrency;
  protected final Options options;
  protected final boolean canUseServerTimeout;
  /**
   * the protocol used to talk to the server.
   */
  protected Protocol protocol;
  /**
   * the  Connection object.
   */
  protected MariaDbConnection connection;
  protected volatile boolean closed = false;
  protected int queryTimeout;
  protected long maxRows;
  protected Results results;
  protected int fetchSize;
  protected volatile boolean executing;
  //are warnings cleared?
  private boolean warningsCleared;
  private boolean mustCloseOnCompletion = false;
  private List<String> batchQueries;
  private Future<?> timerTaskFuture;
  private boolean isTimedout;
  private int maxFieldSize;

  /**
   * Creates a new Statement.
   *
   * @param connection           the connection to return in getConnection.
   * @param resultSetScrollType  one of the following <code>ResultSet</code> constants:
   *                             <code>ResultSet.TYPE_FORWARD_ONLY</code>,
   *                             <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
   *                             <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
   * @param resultSetConcurrency a concurrency type; one of <code>ResultSet.CONCUR_READ_ONLY</code>
   *                             or
   *                             <code>ResultSet.CONCUR_UPDATABLE</code>
   */
  public MariaDbStatement(MariaDbConnection connection, int resultSetScrollType, int resultSetConcurrency) {
    this.protocol = connection.getProtocol();
    this.connection = connection;
    this.canUseServerTimeout = connection.canUseServerTimeout();
    this.resultSetScrollType = resultSetScrollType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.lock = this.connection.lock;
    this.options = this.protocol.getOptions();
    this.fetchSize = this.options.defaultFetchSize;
  }

  /**
   * Clone statement.
   *
   * @param connection connection
   * @return Clone statement.
   * @throws CloneNotSupportedException if any error occur.
   */
  public MariaDbStatement clone(MariaDbConnection connection) throws CloneNotSupportedException {
    MariaDbStatement clone = (MariaDbStatement) super.clone();
    clone.connection = connection;
    clone.protocol = connection.getProtocol();
    clone.timerTaskFuture = null;
    clone.batchQueries = new ArrayList<>();
    clone.closed = false;
    clone.warningsCleared = true;
    clone.maxRows = 0;
    clone.fetchSize = this.options.defaultFetchSize;

    return clone;
  }

  // Part of query prolog - setup timeout timer
  protected void setTimerTask(boolean isBatch) {
    assert (timerTaskFuture == null);

    timerTaskFuture = timeoutScheduler.schedule(() -> {
      try {
        isTimedout = true;
        if (!isBatch) {
          protocol.cancelCurrentQuery();
        }
        protocol.interrupt();
      } catch (Throwable e) {
        //eat
      }
    }, queryTimeout, TimeUnit.SECONDS);
  }

  /**
   * Command prolog.
   * <ol>
   * <li>clear previous query state</li>
   * <li>launch timeout timer if needed</li>
   * </ol>
   *
   * @param isBatch is batch
   * @throws SQLException if statement is closed
   */
  protected void executeQueryPrologue(boolean isBatch) throws SQLException {
    executing = true;
    if (closed) {
      throw new SQLException("execute() is called on closed statement");
    }
    protocol.prolog(maxRows, protocol.getProxy() != null, connection, this);
    if (queryTimeout != 0 && (!canUseServerTimeout || isBatch)) {
      setTimerTask(isBatch);
    }
  }

  private void stopTimeoutTask() {
    if (timerTaskFuture != null) {
      if (!timerTaskFuture.cancel(true)) {
        // could not cancel, task either started or already finished
        // we must now wait for task to finish to ensure state modifications are done
        try {
          timerTaskFuture.get();
        } catch (InterruptedException e) {
          // reset interrupt status
          Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
          // ignore error, likely due to interrupting during cancel
        }
        // we don't catch the exception if already canceled, that would indicate we tried
        // to cancel in parallel (which this code currently is not designed for)
      }
      timerTaskFuture = null;
    }
  }

  /**
   * Reset timeout after query, re-throw SQL exception.
   *
   * @param sqle current exception
   * @return SQLException exception with new message in case of timer timeout.
   */
  protected SQLException executeExceptionEpilogue(SQLException sqle) {
    //if has a failover, closing the statement
    if (sqle.getSQLState() != null && sqle.getSQLState().startsWith("08")) {
      try {
        close();
      } catch (SQLException sqlee) {
        //eat exception
      }
    }

    if (sqle.getErrorCode() == 1148 && !options.allowLocalInfile) {
      return new SQLFeatureNotSupportedException(
              "(conn:" + getServerThreadId() + ") Usage of LOCAL INFILE is disabled. "
              + "To use it enable it via the connection property allowLocalInfile=true",
              "42000",
              1148, sqle);
    }

    if (isTimedout) {
      return new SQLTimeoutException("(conn:" + getServerThreadId() + ") Query timed out", "JZ0002",
          1317, sqle);
    }
    SQLException sqlException = ExceptionMapper
        .getException(sqle, connection, this, queryTimeout != 0);
    logger.error("error executing query", sqlException);
    return sqlException;
  }

  protected void executeEpilogue() {
    stopTimeoutTask();
    isTimedout = false;
    executing = false;
  }


  protected void executeBatchEpilogue() {
    executing = false;
    stopTimeoutTask();
    isTimedout = false;
    clearBatch();
  }

  private SQLException handleFailoverAndTimeout(SQLException sqle) {

    //if has a failover, closing the statement
    if (sqle.getSQLState() != null && sqle.getSQLState().startsWith("08")) {
      try {
        close();
      } catch (SQLException sqlee) {
        //eat exception
      }
    }

    if (isTimedout) {
      sqle = new SQLTimeoutException("(conn:" + getServerThreadId() + ") Query timed out", "JZ0002",
          1317, sqle);
    }
    return sqle;
  }

  protected BatchUpdateException executeBatchExceptionEpilogue(SQLException initialSqle, int size) {
    SQLException sqle = handleFailoverAndTimeout(initialSqle);
    int[] ret;
    if (results == null || !results.commandEnd()) {
      ret = new int[size];
      Arrays.fill(ret, Statement.EXECUTE_FAILED);
    } else {
      ret = results.getCmdInformation().getUpdateCounts();
    }

    sqle = ExceptionMapper.getException(sqle, connection, this, queryTimeout != 0);
    logger.error("error executing query", sqle);

    return new BatchUpdateException(sqle.getMessage(), sqle.getSQLState(), sqle.getErrorCode(), ret,
        sqle);
  }

  /**
   * Executes a query.
   *
   * @param sql               the query
   * @param fetchSize         fetch size
   * @param autoGeneratedKeys a flag indicating whether auto-generated keys should be returned; one
   *                          of
   *                          <code>Statement.RETURN_GENERATED_KEYS</code>
   *                          or <code>Statement.NO_GENERATED_KEYS</code>
   * @return true if there was a result set, false otherwise.
   * @throws SQLException the error description
   */
  private boolean executeInternal(String sql, int fetchSize, int autoGeneratedKeys)
      throws SQLException {

    lock.lock();
    try {

      executeQueryPrologue(false);
      results = new Results(this,
          fetchSize,
          false,
          1,
          false,
          resultSetScrollType,
          resultSetConcurrency,
          autoGeneratedKeys,
          protocol.getAutoIncrementIncrement(),
          sql, null);

      protocol.executeQuery(protocol.isMasterConnection(), results,
          getTimeoutSql(Utils.nativeSql(sql, protocol.noBackslashEscapes())));
      results.commandEnd();
      return results.getResultSet() != null;

    } catch (SQLException exception) {
      throw executeExceptionEpilogue(exception);
    } finally {
      executeEpilogue();
      lock.unlock();
    }

  }

  private String getTimeoutSql(String sql) {
    if (queryTimeout != 0 && canUseServerTimeout) {
      return "SET STATEMENT max_statement_time=" + queryTimeout + " FOR " + sql;
    }
    return sql;
  }

  /**
   * ! This method is for test only ! This permit sending query using specific charset.
   *
   * @param sql     sql
   * @param charset charset
   * @return boolean if execution went well
   * @throws SQLException if any exception occur
   */
  public boolean testExecute(String sql, Charset charset) throws SQLException {
    lock.lock();
    try {

      executeQueryPrologue(false);
      results = new Results(this, fetchSize, false, 1, false, resultSetScrollType,
          resultSetConcurrency, Statement.NO_GENERATED_KEYS, protocol.getAutoIncrementIncrement(), sql, null);
      protocol.executeQuery(protocol.isMasterConnection(), results,
          getTimeoutSql(Utils.nativeSql(sql, protocol.noBackslashEscapes())), charset);
      results.commandEnd();
      return results.getResultSet() != null;

    } catch (SQLException exception) {
      throw executeExceptionEpilogue(exception);
    } finally {
      executeEpilogue();
      lock.unlock();
    }
  }

  /**
   * executes a query.
   *
   * @param sql the query
   * @return true if there was a result set, false otherwise.
   * @throws SQLException if the query could not be sent to server
   */
  public boolean execute(String sql) throws SQLException {
    return executeInternal(sql, fetchSize, Statement.NO_GENERATED_KEYS);
  }

  /**
   * <p>Executes the given SQL statement, which may return multiple results, and signals the driver
   * that any auto-generated keys should be made available for retrieval.  The driver will ignore
   * this signal if the SQL statement is not an <code>INSERT</code> statement, or an SQL statement
   * able to return auto-generated keys (the list of such statements is vendor-specific).</p>
   * <p>In some (uncommon) situations, a single SQL statement may return multiple result sets
   * and/or update counts. Normally you can ignore this unless you are (1) executing a stored
   * procedure that you know may return multiple results or (2) you are dynamically executing an
   * unknown SQL string. </p> The
   * <code>execute</code> method executes an SQL statement and indicates the form of the first
   * result.  You must then use the methods
   * <code>getResultSet</code> or
   * <code>getUpdateCount</code> to retrieve the result, and <code>getInternalMoreResults</code> to
   * move to any subsequent result(s).
   *
   * @param sql               any SQL statement
   * @param autoGeneratedKeys a constant indicating whether auto-generated keys should be made
   *                          available for retrieval using the method<code>getGeneratedKeys</code>;
   *                          one of the following constants:
   *                          <code>Statement.RETURN_GENERATED_KEYS</code> or
   *                          <code>Statement.NO_GENERATED_KEYS</code>
   * @return <code>true</code> if the first result is a <code>ResultSet</code> object;
   *     <code>false</code> if it is an update count or there are no results
   * @throws SQLException                    if a database access error occurs, this method is
   *                                         called on a closed
   *                                         <code>Statement</code> or the second parameter
   *                                         supplied
   *                                         to this method is not <code>Statement.RETURN_GENERATED_KEYS</code>
   *                                         or
   *                                         <code>Statement.NO_GENERATED_KEYS</code>.
   * @see #getResultSet
   * @see #getUpdateCount
   * @see #getMoreResults
   * @see #getGeneratedKeys
   */
  public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException {
    return executeInternal(sql, fetchSize, autoGeneratedKeys);
  }


  /**
   * Executes the given SQL statement, which may return multiple results, and signals the driver
   * that the auto-generated keys indicated in the given array should be made available for
   * retrieval.  This array contains the indexes of the columns in the target table that contain the
   * auto-generated keys that should be made available. The driver will ignore the array if the SQL
   * statement is not an <code>INSERT</code> statement, or an SQL statement able to return
   * auto-generated keys (the list of such statements is vendor-specific).
   * <p>Under some (uncommon) situations, a single SQL statement may return multiple result sets
   * and/or update counts. Normally you can ignore this unless you are (1) executing a stored
   * procedure that you know may return multiple results or (2) you are dynamically executing an
   * unknown SQL string.</p> The
   * <code>execute</code> method executes an SQL statement and indicates the form of the first
   * result. You must then use the methods <code>getResultSet</code> or <code>getUpdateCount</code>
   * to retrieve the result, and <code>getInternalMoreResults</code> to move to any subsequent
   * result(s).
   *
   * @param sql           any SQL statement
   * @param columnIndexes an array of the indexes of the columns in the inserted row that should be
   *                      made available for retrieval by a call to the method
   *                      <code>getGeneratedKeys</code>
   * @return <code>true</code> if the first result is a <code>ResultSet</code> object;
   *     <code>false</code> if it is an update count or there are no results
   * @throws SQLException                    if a database access error occurs, this method is
   *                                         called on a closed
   *                                         <code>Statement</code> or the elements in
   *                                         the <code>int</code> array passed to this method are
   *                                         not valid column indexes
   * @see #getResultSet
   * @see #getUpdateCount
   * @see #getMoreResults
   */
  public boolean execute(final String sql, final int[] columnIndexes) throws SQLException {
    return executeInternal(sql, fetchSize, Statement.RETURN_GENERATED_KEYS);
  }


  /**
   * <p>Executes the given SQL statement, which may return multiple results, and signals the driver
   * that the auto-generated keys indicated in the given array should be made available for
   * retrieval. This array contains the names of the columns in the target table that contain the
   * auto-generated keys that should be made available. The driver will ignore the array if the SQL
   * statement is not an <code>INSERT</code> statement, or an SQL statement able to return
   * auto-generated keys (the list of such statements is vendor-specific). </p>
   * <p> In some (uncommon) situations, a single SQL statement may return multiple result sets
   * and/or update counts. Normally you can ignore this unless you are (1) executing a stored
   * procedure that you know may return multiple results or (2) you are dynamically executing an
   * unknown SQL string. </p>
   * <p>The <code>execute</code> method executes an SQL statement and indicates the form of the
   * first result. You must then use the methods <code>getResultSet</code> or
   * <code>getUpdateCount</code> to retrieve the result, and <code>getInternalMoreResults</code> to
   * move to any subsequent result(s).</p>
   *
   * @param sql         any SQL statement
   * @param columnNames an array of the names of the columns in the inserted row that should be made
   *                    available for retrieval by a call to the method <code>getGeneratedKeys</code>
   * @return <code>true</code> if the next result is a <code>ResultSet</code> object;
   *     <code>false</code> if it is an update count or there are no more results
   * @throws SQLException                    if a database access error occurs, this method is
   *                                         called on a closed
   *                                         <code>Statement</code> or the elements of
   *                                         the <code>String</code> array passed to this method are
   *                                         not valid column names
   * @see #getResultSet
   * @see #getUpdateCount
   * @see #getMoreResults
   * @see #getGeneratedKeys
   */
  public boolean execute(final String sql, final String[] columnNames) throws SQLException {
    return executeInternal(sql, fetchSize, Statement.RETURN_GENERATED_KEYS);
  }


  /**
   * executes a select query.
   *
   * @param sql the query to send to the server
   * @return a result set
   * @throws SQLException if something went wrong
   */
  public ResultSet executeQuery(String sql) throws SQLException {
    if (executeInternal(sql, fetchSize, Statement.NO_GENERATED_KEYS)) {
      return results.getResultSet();
    }
    return SelectResultSet.createEmptyResultSet();
  }


  /**
   * Executes an update.
   *
   * @param sql the update query.
   * @return update count
   * @throws SQLException if the query could not be sent to server.
   */
  public int executeUpdate(String sql) throws SQLException {
    if (executeInternal(sql, fetchSize, Statement.NO_GENERATED_KEYS)) {
      return 0;
    }
    return getUpdateCount();
  }


  /**
   * Executes the given SQL statement and signals the driver with the given flag about whether the
   * auto-generated keys produced by this <code>Statement</code> object should be made available for
   * retrieval.  The driver will ignore the flag if the SQL statement is not an
   * <code>INSERT</code> statement, or an SQL statement able to return auto-generated keys (the
   * list of such statements is vendor-specific).
   *
   * @param sql               an SQL Data Manipulation Language (DML) statement, such as
   *                          <code>INSERT</code>,
   *                          <code>UPDATE</code> or <code>DELETE</code>; or an
   *                          SQL statement that returns nothing, such as a DDL statement.
   * @param autoGeneratedKeys a flag indicating whether auto-generated keys should be made available
   *                          for retrieval; one of the following constants:
   *                          <code>Statement.RETURN_GENERATED_KEYS</code> <code>Statement.NO_GENERATED_KEYS</code>
   * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0
   *     for SQL statements that return nothing
   * @throws SQLException                    if a database access error occurs, this method is
   *                                         called on a closed
   *                                         <code>Statement</code>, the given SQL
   *                                         statement returns a <code>ResultSet</code> object, or
   *                                         the given constant is not one of those allowed
   */
  public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException {
    if (executeInternal(sql, fetchSize, autoGeneratedKeys)) {
      return 0;
    }
    return getUpdateCount();
  }

  /**
   * Executes the given SQL statement and signals the driver that the auto-generated keys indicated
   * in the given array should be made available for retrieval.   This array contains the indexes of
   * the columns in the target table that contain the auto-generated keys that should be made
   * available. The driver will ignore the array if the SQL statement is not an <code>INSERT</code>
   * statement, or an SQL statement able to return auto-generated keys (the list of such statements
   * is vendor-specific).
   *
   * @param sql           an SQL Data Manipulation Language (DML) statement, such as
   *                      <code>INSERT</code>,
   *                      <code>UPDATE</code> or <code>DELETE</code>; or an SQL statement that
   *                      returns nothing, such as a DDL statement.
   * @param columnIndexes an array of column indexes indicating the columns that should be returned
   *                      from the inserted row
   * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0
   *     for SQL statements that return nothing
   * @throws SQLException                    if a database access error occurs, this method is
   *                                         called on a closed
   *                                         <code>Statement</code>, the SQL statement returns a
   *                                         <code>ResultSet</code>
   *                                         object, or the second argument supplied to this method
   *                                         is not an <code>int</code> array whose elements are
   *                                         valid column indexes
   */
  public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException {
    return executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
  }

  /**
   * Executes the given SQL statement and signals the driver that the auto-generated keys indicated
   * in the given array should be made available for retrieval. This array contains the names of the
   * columns in the target table that contain the auto-generated keys that should be made available.
   * The driver will ignore the array if the SQL statement is not an <code>INSERT</code> statement,
   * or an SQL statement able to return auto-generated keys (the list of such statements is
   * vendor-specific).
   *
   * @param sql         an SQL Data Manipulation Language (DML) statement, such as
   *                    <code>INSERT</code>,
   *                    <code>UPDATE</code> or <code>DELETE</code>; or an
   *                    SQL statement that returns nothing, such as a DDL statement.
   * @param columnNames an array of the names of the columns that should be returned from the
   *                    inserted row
   * @return either the row count for <code>INSERT</code>, <code>UPDATE</code>, or
   *     <code>DELETE</code> statements, or 0 for SQL statements that return nothing
   * @throws SQLException                    if a database access error occurs, this method is
   *                                         called on a closed
   *                                         <code>Statement</code>, the SQL statement returns a
   *                                         <code>ResultSet</code>
   *                                         object, or the second argument supplied to this method
   *                                         is not a <code>String</code> array whose elements are
   *                                         valid column names
   */
  public int executeUpdate(final String sql, final String[] columnNames) throws SQLException {
    return executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
  }


  /**
   * Executes the given SQL statement, which may be an INSERT, UPDATE, or DELETE statement or an SQL
   * statement that returns nothing, such as an SQL DDL statement. This method should be used when
   * the returned row count may exceed Integer.MAX_VALUE.
   *
   * @param sql sql command
   * @return update counts
   * @throws SQLException if any error occur during execution
   */
  @Override
  public long executeLargeUpdate(String sql) throws SQLException {
    if (executeInternal(sql, fetchSize, Statement.NO_GENERATED_KEYS)) {
      return 0;
    }
    return getLargeUpdateCount();
  }

  /**
   * Identical to executeLargeUpdate(String sql), with a flag that indicate that autoGeneratedKeys
   * (primary key fields with "auto_increment") generated id's must be retrieved.
   *
   * <p>Those id's will be available using getGeneratedKeys() method.</p>
   *
   * @param sql               sql command
   * @param autoGeneratedKeys a flag indicating whether auto-generated keys should be made available
   *                          for retrieval; one of the following constants: Statement.RETURN_GENERATED_KEYS
   *                          Statement.NO_GENERATED_KEYS
   * @return update counts
   * @throws SQLException if any error occur during execution
   */
  @Override
  public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    if (executeInternal(sql, fetchSize, autoGeneratedKeys)) {
      return 0;
    }
    return getLargeUpdateCount();
  }

  /**
   * Identical to executeLargeUpdate(String sql, int autoGeneratedKeys) with autoGeneratedKeys =
   * Statement.RETURN_GENERATED_KEYS set.
   *
   * @param sql           sql command
   * @param columnIndexes column Indexes
   * @return update counts
   * @throws SQLException if any error occur during execution
   */
  @Override
  public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
    return executeLargeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
  }

  /**
   * Identical to executeLargeUpdate(String sql, int autoGeneratedKeys) with autoGeneratedKeys =
   * Statement.RETURN_GENERATED_KEYS set.
   *
   * @param sql         sql command
   * @param columnNames columns names
   * @return update counts
   * @throws SQLException if any error occur during execution
   */
  @Override
  public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
    return executeLargeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
  }

  /**
   * Releases this <code>Statement</code> object's database and JDBC resources immediately instead
   * of waiting for this to happen when it is automatically closed. It is generally good practice to
   * release resources as soon as you are finished with them to avoid tying up database resources.
   * Calling the method <code>close</code> on a <code>Statement</code> object that is already closed
   * has no effect. <B>Note:</B>When a
   * <code>Statement</code> object is closed, its current <code>ResultSet</code> object, if one
   * exists, is also closed.
   *
   * @throws SQLException if a database access error occurs
   */
  public void close() throws SQLException {
    lock.lock();
    try {
      closed = true;
      if (results != null) {
        if (results.getFetchSize() != 0) {
          skipMoreResults();
        }

        results.close();
      }
      protocol = null;

      if (connection == null || connection.pooledConnection == null
          || connection.pooledConnection.noStmtEventListeners()) {
        return;
      }
      connection.pooledConnection.fireStatementClosed(this);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Retrieves the maximum number of bytes that can be returned for character and binary column
   * values in a <code>ResultSet</code> object produced by this <code>Statement</code> object. This
   * limit applies only to
   * <code>BINARY</code>,
   * <code>VARBINARY</code>,
   * <code>LONGVARBINARY</code>,
   * <code>CHAR</code>,
   * <code>VARCHAR</code>,
   * <code>NCHAR</code>,
   * <code>NVARCHAR</code>,
   * <code>LONGNVARCHAR</code> and
   * <code>LONGVARCHAR</code>
   * columns.  If the limit is exceeded, the excess data is silently discarded.
   *
   * @return the current column size limit for columns storing character and binary values; zero
   *     means there is no limit
   * @see #setMaxFieldSize
   */
  public int getMaxFieldSize() {
    return maxFieldSize;
  }

  /**
   * Sets the limit for the maximum number of bytes that can be returned for character and binary
   * column values in a <code>ResultSet</code> object produced by this <code>Statement</code>
   * object. This limit applies only to <code>BINARY</code>, <code>VARBINARY</code>,
   * <code>LONGVARBINARY</code>, <code>CHAR</code>, <code>VARCHAR</code>, <code>NCHAR</code>,
   * <code>NVARCHAR</code>, <code>LONGNVARCHAR</code> and
   * <code>LONGVARCHAR</code> fields.  If the limit is exceeded, the excess data is silently
   * discarded. For maximum portability, use values greater than 256.
   *
   * @param max the new column size limit in bytes; zero means there is no limit
   * @see #getMaxFieldSize
   */
  public void setMaxFieldSize(final int max) {
    maxFieldSize = max;
  }

  /**
   * Retrieves the maximum number of rows that a <code>ResultSet</code> object produced by this
   * <code>Statement</code> object can contain.  If this limit is exceeded, the excess rows are
   * silently dropped.
   *
   * @return the current maximum number of rows for a <code>ResultSet</code> object produced by this
   * <code>Statement</code> object; zero means there is no limit
   * @see #setMaxRows
   */
  public int getMaxRows() {
    return (int) maxRows;
  }

  /**
   * Sets the limit for the maximum number of rows that any <code>ResultSet</code> object  generated
   * by this <code>Statement</code> object can contain to the given number. If the limit is
   * exceeded, the excess rows are silently dropped.
   *
   * @param max the new max rows limit; zero means there is no limit
   * @throws SQLException if the condition max &gt;= 0 is not satisfied
   * @see #getMaxRows
   */
  public void setMaxRows(final int max) throws SQLException {
    if (max < 0) {
      throw new SQLException("max rows cannot be negative : asked for " + max);
    }
    maxRows = max;
  }

  /**
   * Retrieves the maximum number of rows that a ResultSet object produced by this Statement object
   * can contain. If this limit is exceeded, the excess rows are silently dropped.
   *
   * @return the current maximum number of rows for a ResultSet object produced by this Statement
   *     object; zero means there is no limit
   */
  @Override
  public long getLargeMaxRows() {
    return maxRows;
  }

  /**
   * Sets the limit for the maximum number of rows that any ResultSet object generated by this
   * Statement object can contain to the given number. If the limit is exceeded, the excess rows are
   * silently dropped.
   *
   * @param max the new max rows limit; zero means there is no limit
   * @throws SQLException if the condition max &gt;= 0 is not satisfied
   */
  @Override
  public void setLargeMaxRows(long max) throws SQLException {
    if (max < 0) {
      throw new SQLException("max rows cannot be negative : setLargeMaxRows value is " + max);
    }
    maxRows = max;
  }

  /**
   * Sets escape processing on or off. If escape scanning is on (the default), the driver will do
   * escape substitution before sending the SQL statement to the database. Note: Since prepared
   * statements have usually been parsed prior to making this call, disabling escape processing for
   * <code>PreparedStatements</code> objects will have no effect.
   *
   * @param enable <code>true</code> to enable escape processing; <code>false</code> to disable it
   */
  public void setEscapeProcessing(final boolean enable) {
    //not handled
  }

  /**
   * Retrieves the number of seconds the driver will wait for a <code>Statement</code> object to
   * execute. If the limit is exceeded, a
   * <code>SQLException</code> is thrown.
   *
   * @return the current query timeout limit in seconds; zero means there is no limit
   * @see #setQueryTimeout
   */
  public int getQueryTimeout() {
    return queryTimeout;
  }

  /**
   * Sets the number of seconds the driver will wait for a <code>Statement</code> object to execute
   * to the given number of seconds. If the limit is exceeded, an <code>SQLException</code> is
   * thrown. A JDBC driver must apply this limit to the <code>execute</code>,
   * <code>executeQuery</code> and <code>executeUpdate</code> methods.
   *
   * @param seconds the new query timeout limit in seconds; zero means there is no limit
   * @throws SQLException if a database access error occurs, this method is called on a closed
   *                      <code>Statement</code> or the condition seconds &gt;= 0 is not satisfied
   * @see #getQueryTimeout
   */
  public void setQueryTimeout(final int seconds) throws SQLException {
    if (seconds < 0) {
      throw new SQLException("Query timeout rows cannot be negative : asked for " + seconds);
    }
    this.queryTimeout = seconds;
  }

  /**
   * Sets the inputStream that will be used for the next execute that uses "LOAD DATA LOCAL INFILE".
   * The name specified as local file/URL will be ignored.
   *
   * @param inputStream inputStream instance, that will be used to send data to server
   * @throws SQLException if statement is closed
   */
  public void setLocalInfileInputStream(InputStream inputStream) throws SQLException {
    checkClose();
    protocol.setLocalInfileInputStream(inputStream);
  }

  /**
   * Cancels this <code>Statement</code> object if both the DBMS and driver support aborting an SQL
   * statement. This method can be used by one thread to cancel a statement that is being executed
   * by another thread.
   *
   * <p>In case there is result-set from this Statement that are still streaming data from server, will
   * cancel streaming.</p>
   *
   * @throws SQLException                    if a database access error occurs or this method is
   *                                         called on a closed
   *                                         <code>Statement</code>
   */
  public void cancel() throws SQLException {
    checkClose();
    boolean locked = lock.tryLock();
    try {
      if (executing) {
        protocol.cancelCurrentQuery();
      } else if (results != null && results.getFetchSize() != 0 && !results
          .isFullyLoaded(protocol)) {
        try {
          protocol.cancelCurrentQuery();
          skipMoreResults();
        } catch (SQLException e) {
          //eat exception
        }
        results.removeFetchSize();
      }

    } catch (SQLException e) {
      logger.error("error cancelling query", e);
      ExceptionMapper.throwException(e, connection, this);
    } finally {
      if (locked) {
        lock.unlock();
      }
    }
  }

  /**
   * Retrieves the first warning reported by calls on this <code>Statement</code> object.
   * Subsequent
   * <code>Statement</code> object warnings will be chained to this <code>SQLWarning</code> object.
   * <p>The warning chain is automatically cleared each time a statement is (re)executed. This
   * method may not be called on a closed <code>Statement</code> object; doing so will cause an
   * <code>SQLException</code> to be thrown.</p>
   * <p><B>Note:</B> If you are processing a <code>ResultSet</code> object, any warnings associated
   * with reads on that <code>ResultSet</code> object will be chained on it rather than on the
   * <code>Statement</code> object that produced it.</p>
   *
   * @return the first <code>SQLWarning</code> object or <code>null</code> if there are no warnings
   * @throws SQLException if a database access error occurs or this method is called on a closed
   *                      <code>Statement</code>
   */
  public SQLWarning getWarnings() throws SQLException {
    checkClose();
    if (!warningsCleared) {
      return this.connection.getWarnings();
    }
    return null;
  }

  /**
   * Clears all the warnings reported on this <code>Statement</code> object. After a call to this
   * method, the method <code>getWarnings</code> will return <code>null</code> until a new warning
   * is reported for this <code>Statement</code> object.
   *
   */
  public void clearWarnings() {
    warningsCleared = true;
  }

  /**
   * Sets the SQL cursor name to the given <code>String</code>, which will be used by subsequent
   * <code>Statement</code> object <code>execute</code> methods. This name can then be used in SQL
   * positioned update or delete statements to identify the current row in the
   * <code>ResultSet</code> object generated by this statement.  If the database does not support
   * positioned update/delete, this method is a noop.  To insure that a cursor has the proper
   * isolation level to support updates, the cursor's <code>SELECT</code> statement should have the
   * form <code>SELECT FOR UPDATE</code>.  If <code>FOR UPDATE</code> is not present, positioned
   * updates may fail. <p><B>Note:</B> By definition, the execution of positioned updates and
   * deletes must be done by a different <code>Statement</code> object than the one that generated
   * the <code>ResultSet</code> object being used for positioning. Also, cursor names must be unique
   * within a connection.</p>
   *
   * @param name the new cursor name, which must be unique within a connection
   * @throws SQLException                    if a database access error occurs or this method is
   *                                         called on a closed
   *                                         <code>Statement</code>
   */
  public void setCursorName(final String name) throws SQLException {
    throw ExceptionMapper.getFeatureNotSupportedException("Cursors are not supported");
  }

  /**
   * Gets the connection that created this statement.
   *
   * @return the connection
   */
  public MariaDbConnection getConnection() {
    return this.connection;
  }

  /**
   * Retrieves any auto-generated keys created as a result of executing this <code>Statement</code>
   * object. If this <code>Statement</code> object did not generate any keys, an empty
   * <code>ResultSet</code> object is returned. <p><B>Note:</B>If the columns which represent the
   * auto-generated keys were not specified, the JDBC driver implementation will determine the
   * columns which best represent the auto-generated keys.</p>
   *
   * @return a <code>ResultSet</code> object containing the auto-generated key(s) generated by the
   *     execution of this <code>Statement</code> object
   * @throws SQLException                    if a database access error occurs or this method is
   *                                         called on a closed
   *                                         <code>Statement</code>
   */
  public ResultSet getGeneratedKeys() throws SQLException {
    if (results != null) {
      return results.getGeneratedKeys(protocol);
    }
    return SelectResultSet.createEmptyResultSet();
  }

  /**
   * Retrieves the result set holdability for <code>ResultSet</code> objects generated by this
   * <code>Statement</code> object.
   *
   * @return either <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
   * @since 1.4
   */
  public int getResultSetHoldability() {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  /**
   * Retrieves whether this <code>Statement</code> object has been closed. A <code>Statement</code>
   * is closed if the method close has been called on it, or if it is automatically closed.
   *
   * @return true if this <code>Statement</code> object is closed; false if it is still open
   * @since 1.6
   */
  public boolean isClosed() {
    return closed;
  }

  /**
   * Returns a  value indicating whether the <code>Statement</code> is poolable or not.
   *
   * @return <code>true</code> if the <code>Statement</code> is poolable; <code>false</code>
   *     otherwise
   * @see Statement#setPoolable(boolean) setPoolable(boolean)
   * @since 1.6
   */
  @Override
  public boolean isPoolable() {
    return false;
  }

  /**
   * <p>Requests that a <code>Statement</code> be pooled or not pooled.  The value specified is a
   * hint to the statement pool implementation indicating whether the applicaiton wants the
   * statement to be pooled.  It is up to the statement pool manager as to whether the hint is
   * used.</p> <p> The poolable value of a statement is applicable to both internal statement caches
   * implemented by the driver and external statement caches implemented by application servers and
   * other applications. </p> <p>By default, a
   * <code>Statement</code> is not poolable when created, and a <code>PreparedStatement</code> and
   * <code>CallableStatement</code> are poolable when created.</p>
   *
   * @param poolable requests that the statement be pooled if true and that the statement not be
   *                 pooled if false
   * @since 1.6
   */
  @Override
  public void setPoolable(final boolean poolable) {
    // not handled
  }

  /**
   * Retrieves the current result as a ResultSet object. This method should be called only once per
   * result.
   *
   * @return the current result as a ResultSet object or null if the result is an update count or
   *     there are no more results
   * @throws SQLException if a database access error occurs or this method is called on a closed
   *                      Statement
   */
  public ResultSet getResultSet() throws SQLException {
    checkClose();
    return results != null ? results.getResultSet() : null;
  }

  /**
   * Retrieves the current result as an update count; if the result is a ResultSet object or there
   * are no more results, -1 is returned. This method should be called only once per result.
   *
   * @return the current result as an update count; -1 if the current result is a ResultSet object
   *     or there are no more results
   */
  public int getUpdateCount() {
    if (results != null && results.getCmdInformation() != null && !results.isBatch()) {
      return results.getCmdInformation().getUpdateCount();
    }
    return -1;
  }

  /**
   * Retrieves the current result as an update count; if the result is a ResultSet object or there
   * are no more results, -1 is returned.
   *
   * @return last update count
   */
  @Override
  public long getLargeUpdateCount() {
    if (results != null && results.getCmdInformation() != null && !results.isBatch()) {
      return results.getCmdInformation().getLargeUpdateCount();
    }
    return -1;
  }

  protected void skipMoreResults() throws SQLException {
    try {
      protocol.skip();
      warningsCleared = false;
      connection.reenableWarnings();
    } catch (SQLException e) {
      logger.debug("error skipMoreResults", e);
      ExceptionMapper.throwException(e, connection, this);
    }
  }

  /**
   * <p>Moves to this <code>Statement</code> object's next result, returns <code>true</code> if it
   * is a
   * <code>ResultSet</code> object, and implicitly closes any current <code>ResultSet</code>
   * object(s) obtained with the method <code>getResultSet</code>.</p>
   * There are no more results when the following is true: <pre> // stmt is a Statement object
   * ((stmt.getInternalMoreResults() == false) &amp;&amp; (stmt.getUpdateCount() == -1)) </pre>
   *
   * @return <code>true</code> if the next result is a <code>ResultSet</code> object;
   *     <code>false</code> if it is an update count or there are no more results
   * @throws SQLException if a database access error occurs or this method is called on a closed
   *                      <code>Statement</code>
   * @see #execute
   */
  public boolean getMoreResults() throws SQLException {
    return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
  }

  /**
   * <p>Moves to this <code>Statement</code> object's next result, deals with any current
   * <code>ResultSet</code>
   * object(s) according to the instructions specified by the given flag, and returns
   * <code>true</code> if the next result is a <code>ResultSet</code> object.</p>
   * There are no more results when the following is true: <pre> // stmt is a Statement object
   * ((stmt.getInternalMoreResults(current) == false) &amp;&amp; (stmt.getUpdateCount() == -1))</pre>
   *
   * @param current one of the following <code>Statement</code> constants indicating what should
   *                happen to current
   *                <code>ResultSet</code> objects obtained using the method
   *                <code>getResultSet</code>:
   *                <code>Statement.CLOSE_CURRENT_RESULT</code>, <code>Statement.KEEP_CURRENT_RESULT</code>,
   *                or <code>Statement.CLOSE_ALL_RESULTS</code>
   * @return <code>true</code> if the next result is a <code>ResultSet</code> object;
   *     <code>false</code> if it is an update count or there are no more results
   * @throws SQLException                    if a database access error occurs, this method is
   *                                         called on a closed
   *                                         <code>Statement</code> or the argument supplied is not
   *                                         one of the following:
   *                                         <code>Statement.CLOSE_CURRENT_RESULT</code>,
   *                                         <code>Statement.KEEP_CURRENT_RESULT</code> or
   *                                         <code>Statement.CLOSE_ALL_RESULTS</code>
   * @see #execute
   */
  public boolean getMoreResults(final int current) throws SQLException {
    //if fetch size is set to read fully, other resultSet are put in cache
    checkClose();
    return results != null && results.getMoreResults(current, protocol);
  }


  /**
   * Retrieves the direction for fetching rows from database tables that is the default for result
   * sets generated from this <code>Statement</code> object. If this <code>Statement</code> object
   * has not set a fetch direction by calling the method <code>setFetchDirection</code>, the return
   * value is implementation-specific.
   *
   * @return the default fetch direction for result sets generated from this <code>Statement</code>
   *     object
   * @see #setFetchDirection
   * @since 1.2
   */
  public int getFetchDirection() {
    return ResultSet.FETCH_FORWARD;
  }

  /**
   * <p>Gives the driver a hint as to the direction in which rows will be processed in
   * <code>ResultSet</code> objects created using this
   * <code>Statement</code> object.  The default value is <code>ResultSet.FETCH_FORWARD</code>.</p>
   * <p> Note that this method sets the default fetch
   * direction for result sets generated by this <code>Statement</code> object. Each result set has
   * its own methods for getting and setting its own fetch direction. </p>
   *
   * @param direction the initial direction for processing rows
   * @see #getFetchDirection
   * @since 1.2
   */
  public void setFetchDirection(final int direction) {
    //not implemented
  }

  /**
   * Retrieves the number of result set rows that is the default fetch size for
   * <code>ResultSet</code> objects generated from this
   * <code>Statement</code> object. If this <code>Statement</code> object has not set a fetch size
   * by calling the method <code>setFetchSize</code>, the return value is implementation-specific.
   *
   * @return the default fetch size for result sets generated from this <code>Statement</code>
   *     object
   * @see #setFetchSize
   */
  public int getFetchSize() {
    return this.fetchSize;
  }

  /**
   * Gives the JDBC driver a hint as to the number of rows that should be fetched from the database
   * when more rows are needed for
   * <code>ResultSet</code> objects generated by this <code>Statement</code>. If the value
   * specified is zero, then the hint is ignored. The default value is zero.
   *
   * @param rows the number of rows to fetch
   * @throws SQLException if a database access error occurs, this method is called on a closed
   *                      <code>Statement</code> or the condition
   *                      <code>rows &gt;= 0</code> is not satisfied.
   * @see #getFetchSize
   */
  public void setFetchSize(final int rows) throws SQLException {
    if (rows < 0 && rows != Integer.MIN_VALUE) {
      throw new SQLException("invalid fetch size");
    } else if (rows == Integer.MIN_VALUE) {
      //for compatibility Integer.MIN_VALUE is transform to 0 => streaming
      this.fetchSize = 1;
      return;
    }
    this.fetchSize = rows;
  }

  /**
   * Retrieves the result set concurrency for <code>ResultSet</code> objects generated by this
   * <code>Statement</code> object.
   *
   * @return either <code>ResultSet.CONCUR_READ_ONLY</code> or <code>ResultSet.CONCUR_UPDATABLE</code>
   * @since 1.2
   */
  public int getResultSetConcurrency() {
    return resultSetConcurrency;
  }

  /**
   * Retrieves the result set type for <code>ResultSet</code> objects generated by this
   * <code>Statement</code> object.
   *
   * @return one of <code>ResultSet.TYPE_FORWARD_ONLY</code>, <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>,
   *     or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
   */
  public int getResultSetType() {
    return resultSetScrollType;
  }

  /**
   * Adds the given SQL command to the current list of commands for this <code>Statement</code>
   * object. The send in this list can be executed as a batch by calling the method
   * <code>executeBatch</code>.
   *
   * @param sql typically this is a SQL <code>INSERT</code> or <code>UPDATE</code> statement
   * @throws SQLException if a database access error occurs, this method is called on a closed
   *                      <code>Statement</code> or the driver does not support batch updates
   * @see #executeBatch
   * @see DatabaseMetaData#supportsBatchUpdates
   */
  public void addBatch(final String sql) throws SQLException {
    if (batchQueries == null) {
      batchQueries = new ArrayList<>();
    }
    if (sql == null) {
      throw ExceptionMapper.getSqlException("null cannot be set to addBatch( String sql)");
    }
    batchQueries.add(sql);
  }

  /**
   * Empties this <code>Statement</code> object's current list of SQL send.
   *
   * @see #addBatch
   * @see DatabaseMetaData#supportsBatchUpdates
   * @since 1.2
   */
  public void clearBatch() {
    if (batchQueries != null) {
      batchQueries.clear();
    }
  }

  /**
   * Execute statements. depending on option, queries mays be rewritten :
   *
   * <p>those queries will be rewritten if possible to INSERT INTO ... VALUES (...) ; INSERT INTO ...
   * VALUES (...);</p>
   *
   * <p>if option rewriteBatchedStatements is set to true, rewritten to INSERT INTO ... VALUES (...),
   * (...);</p>
   *
   * @return an array of update counts containing one element for each command in the batch.  The
   *     elements of the array are ordered according to the order in which send were added to the
   *     batch.
   * @throws SQLException if a database access error occurs, this method is called on a closed
   *                      <code>Statement</code> or the driver does not support batch statements.
   *                      Throws {@link BatchUpdateException} (a subclass of
   *                      <code>SQLException</code>) if one of the send sent to the database fails
   *                      to execute properly or attempts to return a result set.
   * @see #addBatch
   * @see DatabaseMetaData#supportsBatchUpdates
   * @since 1.3
   */
  public int[] executeBatch() throws SQLException {
    checkClose();
    int size;
    if (batchQueries == null || (size = batchQueries.size()) == 0) {
      return new int[0];
    }

    lock.lock();
    try {
      internalBatchExecution(size);
      return results.getCmdInformation().getUpdateCounts();
    } catch (SQLException initialSqlEx) {
      throw executeBatchExceptionEpilogue(initialSqlEx, size);
    } finally {
      executeBatchEpilogue();
      lock.unlock();
    }
  }

  /**
   * Execute batch, like executeBatch(), with returning results with long[]. For when row count may
   * exceed Integer.MAX_VALUE.
   *
   * @return an array of update counts (one element for each command in the batch)
   * @throws SQLException if a database error occur.
   */
  @Override
  public long[] executeLargeBatch() throws SQLException {
    checkClose();
    int size;
    if (batchQueries == null || (size = batchQueries.size()) == 0) {
      return new long[0];
    }

    lock.lock();
    try {
      internalBatchExecution(size);
      return results.getCmdInformation().getLargeUpdateCounts();

    } catch (SQLException initialSqlEx) {
      throw executeBatchExceptionEpilogue(initialSqlEx, size);
    } finally {
      executeBatchEpilogue();
      lock.unlock();
    }
  }

  /**
   * Internal batch execution.
   *
   * @param size expected result-set size
   * @throws SQLException throw exception if batch error occur
   */
  private void internalBatchExecution(int size) throws SQLException {

    executeQueryPrologue(true);
    results = new Results(this,
        0,
        true,
        size,
        false,
        resultSetScrollType,
        resultSetConcurrency,
        Statement.RETURN_GENERATED_KEYS,
        protocol.getAutoIncrementIncrement(),
        null,
        null);
    protocol.executeBatchStmt(protocol.isMasterConnection(), results, batchQueries);
    results.commandEnd();
  }

  /**
   * <p>Returns an object that implements the given interface to allow access to non-standard
   * methods, or standard methods not exposed by the proxy.</p>
   * <p>If the receiver implements the interface then the result is the receiver or a proxy for the
   * receiver. If the receiver is a wrapper and the wrapped object implements the interface then the
   * result is the wrapped object or a proxy for the wrapped object. Otherwise return the the result
   * of calling <code>unwrap</code> recursively on the wrapped object or a proxy for that result. If
   * the receiver is not a wrapper and does not implement the interface, then an
   * <code>SQLException</code> is thrown. </p>
   *
   * @param iface A Class defining an interface that the result must implement.
   * @return an object that implements the interface. May be a proxy for the actual implementing
   *     object.
   * @throws SQLException If no object found that implements the interface
   * @since 1.6
   */
  @SuppressWarnings("unchecked")
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    try {
      if (isWrapperFor(iface)) {
        return (T) this;
      } else {
        throw new SQLException(
            "The receiver is not a wrapper and does not implement the interface");
      }
    } catch (Exception e) {
      throw new SQLException("The receiver is not a wrapper and does not implement the interface");
    }
  }

  /**
   * Returns true if this either implements the interface argument or is directly or indirectly a
   * wrapper for an object that does. Returns false otherwise. If this implements the interface then
   * return true, else if this is a wrapper then return the result of recursively calling
   * <code>isWrapperFor</code> on the wrapped object. If this does not implement the interface and
   * is not a wrapper, return false. This method should be implemented as a low-cost operation
   * compared to <code>unwrap</code> so that callers can use this method to avoid expensive
   * <code>unwrap</code> calls that may fail. If this method returns true then calling
   * <code>unwrap</code> with the same argument should succeed.
   *
   * @param interfaceOrWrapper a Class defining an interface.
   * @return true if this implements the interface or directly or indirectly wraps an object that
   *     does.
   * @throws SQLException if an error occurs while determining whether this is a wrapper for an
   *                      object with the given interface.
   * @since 1.6
   */
  public boolean isWrapperFor(final Class<?> interfaceOrWrapper) throws SQLException {
    return interfaceOrWrapper.isInstance(this);
  }

  public void closeOnCompletion() {
    mustCloseOnCompletion = true;
  }

  public boolean isCloseOnCompletion() {
    return mustCloseOnCompletion;
  }

  /**
   * Check that close on completion is asked, and close if so.
   *
   * @param resultSet resultSet
   * @throws SQLException if close has error
   */
  public void checkCloseOnCompletion(ResultSet resultSet) throws SQLException {
    if (mustCloseOnCompletion
        && !closed
        && results != null
        && resultSet.equals(results.getResultSet())) {
      close();
    }
  }

  /**
   * Check if statement is closed, and throw exception if so.
   *
   * @throws SQLException if statement close
   */
  protected void checkClose() throws SQLException {
    if (closed) {
      throw new SQLException("Cannot do an operation on a closed statement");
    }
  }

  /**
   * Permit to retrieve current connection thread id, or -1 if unknown.
   *
   * @return current connection thread id.
   */
  public long getServerThreadId() {
    return (protocol != null) ? protocol.getServerThreadId() : -1;
  }

}
