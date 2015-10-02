/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson, Trond Norbye, Stephane Giron

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/
package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.SQLExceptionMapper;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.query.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.common.queryresults.PrepareResult;
import org.mariadb.jdbc.internal.common.queryresults.ResultSetType;
import org.mariadb.jdbc.internal.mysql.MySQLType;
import org.mariadb.jdbc.internal.mysql.Protocol;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

public class MySQLServerPreparedStatement extends AbstractMySQLPrepareStatement {
    String sql;
    PrepareResult prepareResult;
    boolean returnTableAlias = false;
    int parameterCount;
    MySQLResultSetMetaData metadata;
    MySQLParameterMetaData parameterMetaData;
    ParameterHolder[] currentParameterHolder;
    List<ParameterHolder[]> queryParameters = new ArrayList<>();
    private boolean useFractionalSeconds;

    public MySQLServerPreparedStatement(MySQLConnection connection, String sql) throws SQLException {
        super(connection);
        useFractionalSeconds = connection.getProtocol().getOptions().useFractionalSeconds;
        this.sql = sql;
        prepare(sql);
    }

    private void prepare(String sql) throws SQLException {
        stLock.writeLock().lock();
        try {
            connection.lock.writeLock().lock();
            try {
                if (protocol.hasUnreadData()) {
                    SQLExceptionMapper.throwException(new QueryException("There is an open result set on the current connection, which must be closed prior to executing a query"), connection, this);
                }
                prepareResult = protocol.prepare(sql);
            } finally {
                connection.lock.writeLock().unlock();
            }
            parameterCount = prepareResult.parameters.length;
            if (parameterCount > 0) currentParameterHolder = new ParameterHolder[prepareResult.parameters.length];
            returnTableAlias = protocol.getOptions().useOldAliasMetadataBehavior;
            metadata = new MySQLResultSetMetaData(prepareResult.columns,
                    protocol.getDatatypeMappingFlags(), returnTableAlias);
            parameterMetaData = new MySQLParameterMetaData(prepareResult.columns);
        } catch (QueryException e) {
            try {
                this.close();
            } catch (Exception ee) {
            }
            SQLExceptionMapper.throwException(e, connection, this);
        } finally {
            stLock.writeLock().unlock();
        }
    }

    @Override
    protected boolean isNoBackslashEscapes() {
        return connection.noBackslashEscapes;
    }

    @Override
    protected boolean useFractionalSeconds() {
        return useFractionalSeconds;
    }

    @Override
    protected Calendar cal() {
        return protocol.getCalendar();
    }

    protected void setParameter(final int parameterIndex, final ParameterHolder holder) throws SQLException {
        try {
            stLock.writeLock().lock();
            currentParameterHolder[parameterIndex - 1] = holder;
        } catch (ArrayIndexOutOfBoundsException a) {
            throw SQLExceptionMapper.getSQLException("Could not set parameter at position " + parameterIndex + ", parameter length is " + parameterCount);
        } finally {
            stLock.writeLock().unlock();
        }
    }

    @Override
    public void addBatch() throws SQLException {
        //check
        for (int i = 0; i < parameterCount; i++) {
            if (currentParameterHolder[i] == null)
                SQLExceptionMapper.throwException(new QueryException("Parameter at position " + (i + 1) + " is not set", -1, "07004"), connection, this);
        }

        stLock.writeLock().lock();
        try {
            queryParameters.add(currentParameterHolder.clone());
        } finally {
            stLock.writeLock().unlock();
        }
    }

    public void clearBatch() {
        stLock.writeLock().lock();
        try {
            queryParameters = new ArrayList<>();
        } finally {
            stLock.writeLock().unlock();
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        stLock.readLock().lock();
        try {
            return parameterMetaData;
        } finally {
            stLock.readLock().unlock();
        }
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        stLock.readLock().lock();
        try {
            return metadata;
        } finally {
            stLock.readLock().unlock();
        }
    }


    /**
     * <p>Submits a batch of commands to the database for execution and if all commands execute successfully, returns an
     * array of update counts. The <code>int</code> elements of the array that is returned are ordered to correspond to
     * the commands in the batch, which are ordered according to the order in which they were added to the batch. The
     * elements in the array returned by the method <code>executeBatch</code> may be one of the following:</p>
     * <ol>
     * <li>A number greater than or equal to zero -- indicates that the command was processed successfully and is an update
     * count giving the number of rows in the database that were affected by the command's execution
     * <li>A value of <code>SUCCESS_NO_INFO</code> -- indicates that the command was processed successfully but that the number of rows
     * affected is unknown
     *
     * If one of the commands in a batch update fails to execute properly, this method throws a
     * <code>BatchUpdateException</code>, and a JDBC driver may or may not continue to process the remaining commands in
     * the batch.  However, the driver's behavior must be consistent with a particular DBMS, either always continuing to
     * process commands or never continuing to process commands.  If the driver continues processing after a failure,
     * the array returned by the method <code>BatchUpdateException.getUpdateCounts</code> will contain as many elements
     * as there are commands in the batch, and at least one of the elements will be the following:
     *
     * <LI>A value of <code>EXECUTE_FAILED</code> -- indicates that the command failed to execute successfully and
     * occurs only if a driver continues to process commands after a command fails </ol>
     *
     * The possible implementations and return values have been modified in the Java 2 SDK, Standard Edition, version
     * 1.3 to accommodate the option of continuing to proccess commands in a batch update after a
     * <code>BatchUpdateException</code> object has been thrown.
     *
     * @return an array of update counts containing one element for each command in the batch.  The elements of the
     * array are ordered according to the order in which commands were added to the batch.
     * @throws java.sql.SQLException if a database access error occurs, this method is called on a closed
     *                               <code>Statement</code> or the driver does not support batch statements. Throws
     *                               {@link java.sql.BatchUpdateException} (a subclass of <code>SQLException</code>) if
     *                               one of the commands sent to the database fails to execute properly or attempts to
     *                               return a result set.
     * @see #addBatch
     * @see java.sql.DatabaseMetaData#supportsBatchUpdates
     * @since 1.3
     */
    @Override
    public int[] executeBatch() throws SQLException {
        stLock.writeLock().lock();
        try {
            if (parameterCount > 0 && queryParameters.size() == 0)
                throw SQLExceptionMapper.getSQLException("No Parameters set. The command addBatch() must have been set");
            int i = 0;
            int[] ret = new int[queryParameters.size()];
            MySQLResultSet rs = null;
            MySQLType[] parameterTypeHeader = new MySQLType[parameterCount];
            connection.lock.writeLock().lock();
            try {
                for (; i < queryParameters.size(); i++) {
                    executeInternal(queryParameters.get(i), parameterTypeHeader);
                    int updateCount = getUpdateCount();
                    if (updateCount == -1) {
                        ret[i] = SUCCESS_NO_INFO;
                    } else {
                        ret[i] = updateCount;
                    }
                    if (i == 0) {
                        rs = (MySQLResultSet) getGeneratedKeys();
                    } else {
                        rs = rs.joinResultSets((MySQLResultSet) getGeneratedKeys());
                    }
                }
            } catch (SQLException sqle) {
                clearBatch();
                throw new BatchUpdateException(sqle.getMessage(), sqle.getSQLState(), sqle.getErrorCode(), Arrays.copyOf(ret, i), sqle);
            } finally {
                connection.lock.writeLock().unlock();
                clearBatch();
            }
            batchResultSet = rs;
            return ret;
        } finally {
            stLock.writeLock().unlock();
        }
    }


    private boolean executeInternal(ParameterHolder[] parameters, MySQLType[] parameterTypeHeader) throws SQLException {
        stLock.writeLock().lock();
        try {
            executing = true;
            QueryException exception = null;
            connection.lock.writeLock().lock();
            try {
                executeQueryProlog();
                try {
                    batchResultSet = null;
                    queryResult = protocol.executePreparedQuery(sql, parameters, prepareResult, parameterTypeHeader, isNotLockedStreaming());

                    // in case of failover
                    if (queryResult.getFailureObject() != null) {
                        prepareResult = queryResult.getFailureObject();
                    }
                    cacheMoreResults();
                    return (queryResult.getResultSetType() == ResultSetType.SELECT);
                } catch (QueryException e) {
                    exception = e;
                    return false;
                } finally {
                    executeQueryEpilog(exception, sql);
                    executing = false;
                }
            } finally {
                connection.lock.writeLock().unlock();
            }
        } finally {
            stLock.writeLock().unlock();
        }
    }


    /*
     Reset timeout after query, re-throw  SQL  exception
    */
    private void executeQueryEpilog(QueryException e, String sql) throws SQLException {

        if (timerTask != null) {
            timerTask.cancel();
            timerTask = null;
        }

        if (isTimedout) {
            isTimedout = false;
            e = new QueryException("Query timed out", 1317, "JZ0002", e);
        }

        if (e == null)
            return;

        /* Include query into exception message, if dumpQueriesOnException is true,
         * or on SQL syntax error (MySQL error code 1064).
         *
         * If SQL query is too long, truncate it to reasonable (for exception messages)
         * length.
         */
        if (protocol.getOptions().dumpQueriesOnException
                || e.getErrorCode() == 1064) {
            String queryString = new String(sql);
            if (queryString.length() > 4096) {
                queryString = queryString.substring(0, 4096);
            }
            e.setMessage(e.getMessage() + "\nQuery is:\n" + queryString);
        }

        //if has a failover, closing the statement
        if (e.getSqlState() != null && e.getSqlState().startsWith("08")) close();

        SQLExceptionMapper.throwException(e, connection, this);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        execute();
        return getResultSet();
    }

    @Override
    public int executeUpdate() throws SQLException {
        execute();
        return getUpdateCount();
    }

    @Override
    public void clearParameters() throws SQLException {
        stLock.writeLock().lock();
        try {
            currentParameterHolder = new ParameterHolder[prepareResult.parameters.length];
        } finally {
            stLock.writeLock().unlock();
        }
    }

    @Override
    public boolean execute() throws SQLException {
        stLock.writeLock().lock();
        try {
            for (int i = 0; i < parameterCount; i++) {
                if (currentParameterHolder[i] == null)
                    SQLExceptionMapper.throwException(new QueryException("Parameter at position " + (i + 1) + " is not set", -1, "07004"), connection, this);

            }
            MySQLResultSet rs = null;
            boolean result = false;
            connection.lock.writeLock().lock();
            try {
                result = executeInternal(currentParameterHolder, new MySQLType[parameterCount]);
                rs = (MySQLResultSet) getGeneratedKeys();
            } catch (SQLException sqle) {
                throw new BatchUpdateException(sqle.getMessage(), sqle.getSQLState(), sqle.getErrorCode(), new int[]{0}, sqle);
            } finally {
                connection.lock.writeLock().unlock();
                clearBatch();
            }
            batchResultSet = rs;
            return result;
        } finally {
            stLock.writeLock().unlock();
        }
    }


    /**
     * <p>Releases this <code>Statement</code> object's database and JDBC resources immediately instead of waiting for this
     * to happen when it is automatically closed. It is generally good practice to release resources as soon as you are
     * finished with them to avoid tying up database resources.</p>
     *
     * <p>Calling the method <code>close</code> on a <code>Statement</code> object that is already closed has no effect.</p>
     *
     * <p><B>Note:</B>When a <code>Statement</code> object is closed, its current <code>ResultSet</code> object, if one
     * exists, is also closed.</p>
     *
     * @throws java.sql.SQLException if a database access error occurs
     */
    @Override
    public void close() throws SQLException {
        stLock.writeLock().lock();
        try {
            if (queryResult != null) {
                queryResult.close();
                queryResult = null;
            }
            // No possible future use for the cached results, so these can be cleared
            // This makes the cache eligible for garbage collection earlier if the statement is not
            // immediately garbage collected
            cachedResultSets.clear();

            if (protocol.isConnected()) {
                try {
                    protocol.releasePrepareStatement(sql, prepareResult.statementId);
                } catch (QueryException e) {
                    //if (log.isDebugEnabled()) log.debug("Error releasing preparedStatement", e);
                }
            }

            if (isNotLockedStreaming()) {
                connection.lock.writeLock().lock();
                try {
                    while (getMoreResults(true)) {
                    }
                } finally {
                    connection.lock.writeLock().unlock();
                }
            }
            super.close();
            isClosed = true;

            if (connection == null || connection.pooledConnection == null || connection.pooledConnection.statementEventListeners.isEmpty()) {
                return;
            }
            connection.pooledConnection.fireStatementClosed(this);

        } finally {
            stLock.writeLock().unlock();
        }
    }


    public String toString() {
        StringBuffer sb = new StringBuffer("sql : '" + sql + "'");
        if (parameterCount > 0) {
            sb.append(", parameters : [");
            for (int i = 0; i < parameterCount; i++) {
                if (currentParameterHolder[i] == null) {
                    sb.append("null");
                } else {
                    sb.append(currentParameterHolder[i].toString());
                }
                if (i != parameterCount - 1) {
                    sb.append(",");
                }
            }
            sb.append("]");
        }
        return sb.toString();
    }
}
