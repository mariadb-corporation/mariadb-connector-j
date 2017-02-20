package org.mariadb.jdbc;
/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.

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

import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.queryresults.*;
import org.mariadb.jdbc.internal.queryresults.resultset.SelectResultSetCommon;
import org.mariadb.jdbc.internal.util.ExceptionMapper;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;

import java.sql.*;
import java.util.*;

public abstract class BasePreparedStatementServer extends BasePrepareStatement implements Cloneable {

    private static Logger logger = LoggerFactory.getLogger(BasePreparedStatementServer.class);

    String sql;
    ServerPrepareResult serverPrepareResult = null;
    boolean returnTableAlias = false;
    int parameterCount = -1;
    MariaDbResultSetMetaData metadata;
    MariaDbParameterMetaData parameterMetaData;
    Map<Integer, ParameterHolder> currentParameterHolder;
    List<ParameterHolder[]> queryParameters = new ArrayList<>();
    boolean mustExecuteOnMaster;

    /**
     * Constructor for creating Server prepared statement.
     *
     * @param connection          current connection
     * @param sql                 Sql String to prepare
     * @param resultSetScrollType one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                            <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param forcePrepare        force immediate prepare
     * @throws SQLException exception
     */
    public BasePreparedStatementServer(MariaDbConnection connection, String sql, int resultSetScrollType, boolean forcePrepare)
            throws SQLException {
        super(connection, resultSetScrollType);
        this.sql = sql;
        returnTableAlias = options.useOldAliasMetadataBehavior;
        currentParameterHolder = Collections.synchronizedMap(new TreeMap<Integer, ParameterHolder>());
        mustExecuteOnMaster = protocol.isMasterConnection();
        if (forcePrepare) prepare(this.sql);
    }

    /**
     * Clone statement.
     *
     * @return Clone statement.
     * @throws CloneNotSupportedException if any error occur.
     */
    public BasePreparedStatementServer clone() throws CloneNotSupportedException {
        BasePreparedStatementServer clone = (BasePreparedStatementServer) super.clone();
        clone.metadata = metadata;
        clone.parameterMetaData = parameterMetaData;
        clone.queryParameters = new ArrayList<>();
        clone.mustExecuteOnMaster = mustExecuteOnMaster;
        //force prepare
        try {
            clone.prepare(sql);
        } catch (SQLException e) {
            throw new CloneNotSupportedException("PrepareStatement not ");
        }
        return clone;
    }

    private void prepare(String sql) throws SQLException {
        try {
            serverPrepareResult = protocol.prepare(sql, mustExecuteOnMaster);
            setMetaFromResult();
        } catch (SQLException e) {
            try {
                this.close();
            } catch (Exception ee) {
                //eat exception.
            }
            logger.error("error preparing query", e);
            throw ExceptionMapper.getException(e, connection, this, false);
        }
    }

    private void setMetaFromResult() {
        parameterCount = serverPrepareResult.getParameters().length;
        metadata = new MariaDbResultSetMetaData(serverPrepareResult.getColumns(), protocol.getDataTypeMappingFlags(), returnTableAlias);
        parameterMetaData = new MariaDbParameterMetaData(serverPrepareResult.getParameters());
        sql = null;
    }

    protected void setParameter(final int parameterIndex, final ParameterHolder holder) throws SQLException {
        currentParameterHolder.put(parameterIndex - 1, holder);
    }

    @Override
    public void addBatch() throws SQLException {
        validParameters();
        queryParameters.add(currentParameterHolder.values().toArray(new ParameterHolder[0]));
    }

    /**
     * Add batch.
     *
     * @param sql typically this is a SQL <code>INSERT</code> or <code>UPDATE</code> statement
     * @throws SQLException every time since that method is forbidden on prepareStatement
     */
    @Override
    public void addBatch(final String sql) throws SQLException {
        throw new SQLException("Cannot do addBatch(String) on preparedStatement");
    }

    public void clearBatch() {
        queryParameters.clear();
        hasLongData = false;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        if (serverPrepareResult == null) prepare(sql);
        return parameterMetaData;
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (serverPrepareResult == null) prepare(sql);
        return metadata;
    }


    /**
     * <p>Submits a batch of send to the database for execution and if all send execute successfully, returns an
     * array of update counts. The <code>int</code> elements of the array that is returned are ordered to correspond to
     * the send in the batch, which are ordered according to the order in which they were added to the batch. The
     * elements in the array returned by the method <code>executeBatch</code> may be one of the following:</p>
     * <ol><li>A number greater than or equal to zero -- indicates that the command was processed successfully and is an update
     * count giving the number of rows in the database that were affected by the command's execution
     * <li>A value of <code>SUCCESS_NO_INFO</code> -- indicates that the command was processed successfully but that the number of rows
     * affected is unknown.
     * If one of the send in a batch update fails to execute properly, this method throws a
     * <code>BatchUpdateException</code>, and a JDBC driver may or may not continue to process the remaining send in
     * the batch.  However, the driver's behavior must be consistent with a particular DBMS, either always continuing to
     * process send or never continuing to process send.  If the driver continues processing after a failure,
     * the array returned by the method <code>BatchUpdateException.getUpdateCounts</code> will contain as many elements
     * as there are send in the batch, and at least one of the elements will be the following:
     * <li>A value of <code>EXECUTE_FAILED</code> -- indicates that the command failed to execute successfully and
     * occurs only if a driver continues to process send after a command fails </ol>
     * <p>The possible implementations and return values have been modified in the Java 2 SDK, Standard Edition, version
     * 1.3 to accommodate the option of continuing to proccess send in a batch update after a
     * <code>BatchUpdateException</code> object has been thrown.</p>
     *
     * @return an array of update counts containing one element for each command in the batch.  The elements of the
     * array are ordered according to the order in which send were added to the batch.
     * @throws SQLException if a database access error occurs, this method is called on a closed
     *                      <code>Statement</code> or the driver does not support batch statements. Throws
     *                      {@link BatchUpdateException} (a subclass of <code>SQLException</code>) if
     *                      one of the send sent to the database fails to execute properly or attempts to
     *                      return a result set.
     * @see #addBatch
     * @see DatabaseMetaData#supportsBatchUpdates
     * @since 1.3
     */
    @Override
    public int[] executeBatch() throws SQLException {
        checkClose();
        int queryParameterSize = queryParameters.size();
        if (queryParameterSize == 0) return new int[0];

        lock.lock();
        try {

            executeBatchInternal(results, queryParameterSize);

            return results.getCmdInformation().getUpdateCounts();

        } catch (SQLException initialSqlEx) {
            throw executeBatchExceptionEpilogue(initialSqlEx, results.getCmdInformation(), queryParameterSize);
        } finally {
            executeBatchEpilogue();
            lock.unlock();
        }

    }

    /**
     * Send batch datas according to options.
     *
     * @param results                 results.
     * @param queryParameterSize      batch size
     * @throws SQLException if any error occur.
     */
    protected void executeBatchInternal(Results results, int queryParameterSize)
            throws SQLException {
        executeQueryPrologue(serverPrepareResult);

        results.reset(0, true, queryParameterSize, true,resultSetScrollType);;

        //if  multi send capacity
        if (options.useBatchMultiSend) {
            //send all sub-command in one packet (or more if > max_allowed_packet)
            serverPrepareResult = protocol.prepareAndExecutes(mustExecuteOnMaster, serverPrepareResult, results, sql, queryParameters);
            if (metadata == null) setMetaFromResult(); //first prepare
            return;
        }

        //send query one by one, reading results for each query before sending another one
        SQLException exception = null;
        for (int counter = 0; counter < queryParameterSize; counter++) {
            ParameterHolder[] parameterHolder = queryParameters.get(counter);
            try {
                serverPrepareResult.resetParameterTypeHeader();
                protocol.executePreparedQuery(mustExecuteOnMaster, serverPrepareResult, results, parameterHolder);
            } catch (SQLException queryException) {
                if (options.continueBatchOnError) {
                    if (exception == null) exception = queryException;
                } else {
                    throw queryException;
                }
            }
        }
        if (exception != null) throw exception;

        results.commandEnd();
    }

    // must have "lock" locked before invoking
    private void executeQueryPrologue(ServerPrepareResult serverPrepareResult) throws SQLException {
        executing = true;
        if (closed) {
            throw new SQLException("execute() is called on closed statement");
        }
        protocol.prologProxy(serverPrepareResult, maxRows, protocol.getProxy() != null, connection, this);
        if (queryTimeout != 0) setTimerTask();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        if (execute()) {
            return results.getResultSet();
        }
        return MariaSelectResultSet.createEmptyResultSet();
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (execute()) {
            return 0;
        }
        return getUpdateCount();
    }

    @Override
    public void clearParameters() throws SQLException {
        currentParameterHolder.clear();
    }

    @Override
    public boolean execute() throws SQLException {
        return executeInternal(getFetchSize());
    }

    protected void validParameters() throws SQLException {
        if (serverPrepareResult != null) {
            for (int i = 0; i < parameterCount; i++) {
                if (currentParameterHolder.get(i) == null) {
                    logger.error("Parameter at position " + (i + 1) + " is not set");
                    ExceptionMapper.throwException(new SQLException("Parameter at position " + (i + 1) + " is not set", "07004"),
                            connection, this);
                }
            }
        } else {
            if (parameterCount == -1) parameterCount = currentParameterHolder.size();
            for (int i = 0; i < parameterCount; i++) {
                if (!currentParameterHolder.containsKey(i)) {
                    parameterCount = -1;
                    logger.error("Parameter at position " + (i + 1) + " is not set");
                    ExceptionMapper.throwException(new SQLException("Parameter at position " + (i + 1) + " is not set", "07004"),
                            connection, this);
                }
            }
        }
    }

    protected boolean executeInternal(int fetchSize) throws SQLException {
        validParameters();

        lock.lock();
        try {
            executeQueryPrologue(serverPrepareResult);
            ParameterHolder[] parameterHolders = currentParameterHolder.values().toArray(new ParameterHolder[0]);

            results.reset(fetchSize, false,1,  true,resultSetScrollType);

            if (serverPrepareResult != null) {
                serverPrepareResult.resetParameterTypeHeader();
                protocol.executePreparedQuery(mustExecuteOnMaster, serverPrepareResult, results, parameterHolders);
            } else {
                serverPrepareResult = protocol.prepareAndExecute(mustExecuteOnMaster, null, results, sql, parameterHolders);
                setMetaFromResult();
            }

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
     * <p>Releases this <code>Statement</code> object's database and JDBC resources immediately instead of waiting for this
     * to happen when it is automatically closed. It is generally good practice to release resources as soon as you are
     * finished with them to avoid tying up database resources.</p>
     * <p>Calling the method <code>close</code> on a <code>Statement</code> object that is already closed has no effect.</p>
     * <p><B>Note:</B>When a <code>Statement</code> object is closed, its current <code>ResultSet</code> object, if one
     * exists, is also closed.</p>
     *
     * @throws SQLException if a database access error occurs
     */
    @Override
    public void close() throws SQLException {
        lock.lock();
        try {
            closed = true;

            if (results.getFetchSize() != 0) skipMoreResults();
            results.close();

            // No possible future use for the cached results, so these can be cleared
            // This makes the cache eligible for garbage collection earlier if the statement is not
            // immediately garbage collected
            if (serverPrepareResult != null && protocol != null) {
                try {
                    serverPrepareResult.getUnProxiedProtocol().releasePrepareStatement(serverPrepareResult);
                } catch (SQLException e) {
                    //if (log.isDebugEnabled()) log.debug("Error releasing preparedStatement", e);
                }
            }
            serverPrepareResult = null;
            protocol = null;
            if (connection == null || connection.pooledConnection == null
                    || connection.pooledConnection.statementEventListeners.isEmpty()) {
                return;
            }
            connection.pooledConnection.fireStatementClosed(this);
            connection = null;
        } finally {
            lock.unlock();
        }
    }

    protected int getParameterCount() {
        return parameterCount;
    }

    /**
     * Return sql String value.
     *
     * @return String representation
     */
    public String toString() {
        StringBuffer sb;
        if (serverPrepareResult != null) {
            sb = new StringBuffer("sql : '" + serverPrepareResult.getSql() + "'");
            if (parameterCount > 0) {
                sb.append(", parameters : [");
                for (int i = 0; i < parameterCount; i++) {
                    ParameterHolder holder = currentParameterHolder.get(i);
                    if (holder == null) {
                        sb.append("null");
                    } else {
                        sb.append(holder.toString());
                    }
                    if (i != parameterCount - 1) {
                        sb.append(",");
                    }
                }
                sb.append("]");
            }
        } else {
            sb = new StringBuffer("sql : '" + sql + "'");
            sb.append(", parameters : [");
            for (int i = 0; i < currentParameterHolder.size(); i++) {
                ParameterHolder holder = currentParameterHolder.get(i);
                if (holder == null) {
                    sb.append("null");
                } else {
                    sb.append(holder.toString());
                }
                if (i != currentParameterHolder.size() - 1) {
                    sb.append(",");
                }
            }
            sb.append("]");
        }
        return sb.toString();
    }

    /**
     * Permit to retrieve current connection thread id, or -1 if unknown.
     *
     * @return current connection thread id.
     */
    public long getServerThreadId() {
        if (serverPrepareResult != null) {
            return serverPrepareResult.getUnProxiedProtocol().getServerThreadId();
        }
        return (protocol != null) ? protocol.getServerThreadId() : -1;
    }

}
