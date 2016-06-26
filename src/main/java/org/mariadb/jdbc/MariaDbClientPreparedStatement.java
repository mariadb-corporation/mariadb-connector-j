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

import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.queryresults.MultiIntExecutionResult;
import org.mariadb.jdbc.internal.queryresults.SingleExecutionResult;
import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;
import org.mariadb.jdbc.internal.util.ExceptionMapper;
import org.mariadb.jdbc.internal.util.Utils;
import org.mariadb.jdbc.internal.util.dao.ClientPrepareResult;
import org.mariadb.jdbc.internal.util.dao.QueryException;

import java.sql.*;
import java.util.*;


public class MariaDbClientPreparedStatement extends AbstractMariaDbPrepareStatement implements Cloneable {
    private String sqlQuery;
    private ClientPrepareResult prepareResult;
    private ParameterHolder[] parameters;
    private List<ParameterHolder[]> parameterList = new ArrayList<>();
    private ResultSetMetaData resultSetMetaData = null;
    private ParameterMetaData parameterMetaData = null;
    private RewriteType rewriteType;

    enum RewriteType {
        NO_REWRITE, /* inside  query */
        MULTI_QUERIES, /* inside string */
        REWRITE_QUERIES
    }

    /**
     * Constructor.
     * @param connection connection
     * @param sql sql query
     * @param resultSetScrollType one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     * <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @throws SQLException exception
     */
    public MariaDbClientPreparedStatement(MariaDbConnection connection, String sql, int resultSetScrollType) throws SQLException {
        super(connection, resultSetScrollType);
        this.sqlQuery = Utils.nativeSql(sql, connection.noBackslashEscapes);
        useFractionalSeconds = options.useFractionalSeconds;

        if (options.cachePrepStmts) {
            String key = new StringBuilder(this.protocol.getDatabase()).append("-").append(sqlQuery).toString();
            prepareResult = connection.getClientPrepareStatementCache().get(key);
        }
        rewriteType = options.rewriteBatchedStatements
                ? RewriteType.REWRITE_QUERIES : (options.allowMultiQueries ? RewriteType.MULTI_QUERIES : RewriteType.NO_REWRITE);

        if (prepareResult == null) {
            if (rewriteType == RewriteType.REWRITE_QUERIES) {
                prepareResult = ClientPrepareResult.createRewritableParts(sqlQuery, connection.noBackslashEscapes);
            } else {
                prepareResult = ClientPrepareResult.createParameterParts(sqlQuery, connection.noBackslashEscapes);
            }
            if (options.cachePrepStmts && sql.length() < 1024) {
                String key = new StringBuilder(this.protocol.getDatabase()).append("-").append(sqlQuery).toString();
                connection.getClientPrepareStatementCache().put(key, prepareResult);
            }
        }
        parameters = new ParameterHolder[prepareResult.getParamCount()];
    }

    /**
     * Clone statement.
     *
     * @return Clone statement.
     * @throws CloneNotSupportedException if any error occur.
     */
    public MariaDbClientPreparedStatement clone() throws CloneNotSupportedException {
        MariaDbClientPreparedStatement clone = (MariaDbClientPreparedStatement) super.clone();
        clone.sqlQuery = sqlQuery;
        clone.prepareResult = prepareResult;
        clone.parameters = new ParameterHolder[prepareResult.getParamCount()];
        clone.resultSetMetaData = resultSetMetaData;
        clone.parameterMetaData = parameterMetaData;
        return clone;
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

    /**
     * Executes the SQL statement in this <code>PreparedStatement</code> object,
     * which may be any kind of SQL statement.
     * Some prepared statements return multiple results; the <code>execute</code>
     * method handles these complex statements as well as the simpler
     * form of statements handled by the methods <code>executeQuery</code>
     * and <code>executeUpdate</code>.
     * <br>
     * The <code>execute</code> method returns a <code>boolean</code> to
     * indicate the form of the first result.  You must call either the method
     * <code>getResultSet</code> or <code>getUpdateCount</code>
     * to retrieve the result; you must call <code>getInternalMoreResults</code> to
     * move to any subsequent result(s).
     *
     * @return <code>true</code> if the first result is a <code>ResultSet</code>
     * object; <code>false</code> if the first result is an update
     * count or there is no result
     * @throws SQLException if a database access error occurs;
     *                               this method is called on a closed <code>PreparedStatement</code>
     *                               or an argument is supplied to this method
     * @see Statement#execute
     * @see Statement#getResultSet
     * @see Statement#getUpdateCount
     * @see Statement#getMoreResults
     */
    public boolean execute() throws SQLException {
        return executeInternal();
    }

    /**
     * Executes the SQL query in this <code>PreparedStatement</code> object
     * and returns the <code>ResultSet</code> object generated by the query.
     *
     * @return a <code>ResultSet</code> object that contains the data produced by the
     * query; never <code>null</code>
     * @throws SQLException if a database access error occurs;
     *                               this method is called on a closed  <code>PreparedStatement</code> or the SQL
     *                               statement does not return a <code>ResultSet</code> object
     */
    public ResultSet executeQuery() throws SQLException {
        if (executeInternal()) {
            return executionResult.getResultSet();
        }
        return MariaSelectResultSet.EMPTY;
    }



    /**
     * Executes the SQL statement in this <code>PreparedStatement</code> object, which must be an SQL Data Manipulation
     * Language (DML) statement, such as <code>INSERT</code>, <code>UPDATE</code> or <code>DELETE</code>; or an SQL
     * statement that returns nothing, such as a DDL statement.
     *
     * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for SQL statements
     * that return nothing
     * @throws SQLException if a database access error occurs; this method is called on a closed
     *                               <code>PreparedStatement</code> or the SQL statement returns a
     *                               <code>ResultSet</code> object
     */
    public int executeUpdate() throws SQLException {
        if (executeInternal()) {
            return 0;
        }
        return getUpdateCount();
    }


    protected boolean executeInternal() throws SQLException {
        executing = true;
        QueryException exception = null;

        //valid parameters
        for (int i = 0; i < prepareResult.getParamCount(); i++) {
            if (parameters[i] == null) {
                throw ExceptionMapper.getSqlException("You need to set exactly " + prepareResult.getParamCount()
                        + " parameters on the prepared statement");
            }
        }

        lock.lock();
        try {
            executeQueryProlog();
            batchResultSet = null;
            SingleExecutionResult executionResultTmp = new SingleExecutionResult(this, getFetchSize(), true, false, true);
            if (rewriteType == RewriteType.REWRITE_QUERIES) {
                protocol.executeQuery(protocol.isMasterConnection(), executionResultTmp, prepareResult.getQueryParts(),
                        parameters, resultSetScrollType, false);
            } else {
                protocol.executeQuery(protocol.isMasterConnection(), executionResultTmp, prepareResult.getQueryParts(),
                        parameters, resultSetScrollType);
            }
            executionResult = executionResultTmp;
            return executionResult.getResultSet() != null;
        } catch (QueryException e) {
            exception = e;
            return false;
        } finally {
            lock.unlock();
            executeQueryEpilog(exception);
            executing = false;
        }
    }

    /**
     * Adds a set of parameters to this <code>PreparedStatement</code> object's batch of send.
     * <br>
     * <br>
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @see Statement#addBatch
     * @since 1.2
     */
    public void addBatch() throws SQLException {
        ParameterHolder[] holder = new ParameterHolder[prepareResult.getParamCount()];
        for (int i = 0; i < holder.length; i++) {
            holder[i] = parameters[i];
            if (holder[i] == null) {
                throw ExceptionMapper.getSqlException("You need to set exactly " + prepareResult.getParamCount()
                        + " parameters on the prepared statement");
            }
        }
        parameterList.add(holder);
    }

    /**
     * Add batch.
     * @param sql typically this is a SQL <code>INSERT</code> or <code>UPDATE</code> statement
     * @throws SQLException every time since that method is forbidden on prepareStatement
     */
    @Override
    public void addBatch(final String sql) throws SQLException {
        throw new SQLException("Cannot do addBatch(String) on preparedStatement");
    }

    /**
     * Clear batch.
     */
    @Override
    public void clearBatch() {
        parameterList.clear();
        this.parameters = new ParameterHolder[prepareResult.getParamCount()];
    }

    /**
     * {inheritdoc}.
     */
    public int[] executeBatch() throws SQLException {
        checkClose();
        int size = parameterList.size();
        if (size == 0) {
            return new int[0];
        }

        MultiIntExecutionResult internalExecutionResult = new MultiIntExecutionResult(this, size, 0, false);

        lock.lock();
        try {
            QueryException exception = null;
            executeQueryProlog();
            try {
                if (rewriteType == RewriteType.REWRITE_QUERIES) {
                    if (prepareResult.isRewritableValuesQuery()) {
                        protocol.executeBatchRewrite(protocol.isMasterConnection(), internalExecutionResult, prepareResult.getQueryParts(),
                                parameterList, resultSetScrollType, true);
                        internalExecutionResult.updateResultsForRewrite(false);
                    } else if (prepareResult.isRewritableMultipleQuery()) {
                        protocol.executeBatchRewrite(protocol.isMasterConnection(), internalExecutionResult, prepareResult.getQueryParts(),
                                parameterList, resultSetScrollType, false);
                        internalExecutionResult.updateResultsMultiple(internalExecutionResult.getCachedExecutionResults(), false);
                    } else {
                        for (int batchQueriesCount = 0; batchQueriesCount < size; batchQueriesCount++) {
                            protocol.executeQuery(protocol.isMasterConnection(), internalExecutionResult, prepareResult.getQueryParts(),
                                    parameterList.get(batchQueriesCount),
                                    resultSetScrollType, false);
                        }
                    }
                } else {
                    if (prepareResult.isRewritableMultipleQuery() && rewriteType == RewriteType.MULTI_QUERIES) {
                        protocol.executeBatchMultiple(protocol.isMasterConnection(), internalExecutionResult, prepareResult.getQueryParts(),
                                parameterList, resultSetScrollType);
                        internalExecutionResult.updateResultsMultiple(internalExecutionResult.getCachedExecutionResults(), false);
                    } else {
                        for (int batchQueriesCount = 0; batchQueriesCount < size; batchQueriesCount++) {
                            protocol.executeQuery(protocol.isMasterConnection(), internalExecutionResult, prepareResult.getQueryParts(),
                                    parameterList.get(batchQueriesCount), resultSetScrollType);
                        }
                    }
                }
            } catch (QueryException e) {
                exception = e;
            } finally {
                if (exception != null) {
                    if (rewriteType == RewriteType.REWRITE_QUERIES) {
                        if (prepareResult.isRewritableValuesQuery()) {
                            internalExecutionResult.updateResultsForRewrite(true);
                        } else if (prepareResult.isRewritableMultipleQuery()) {
                            internalExecutionResult.updateResultsMultiple(internalExecutionResult.getCachedExecutionResults(), true);
                        }
                    } else if (prepareResult.isRewritableMultipleQuery() && rewriteType == RewriteType.MULTI_QUERIES) {
                        internalExecutionResult.updateResultsMultiple(internalExecutionResult.getCachedExecutionResults(), true);
                    }
                }

                executionResult = internalExecutionResult;
                executing = false;
                executeQueryEpilog(exception);
            }

        } catch (SQLException sqle) {
            throw new BatchUpdateException(sqle.getMessage(), sqle.getSQLState(), sqle.getErrorCode(),
                    internalExecutionResult.getAffectedRows(), sqle);
        } finally {
            lock.unlock();
            clearBatch();
        }
        return internalExecutionResult.getAffectedRows();
    }

    /**
     * Retrieves a <code>ResultSetMetaData</code> object that contains information about the columns of the
     * <code>ResultSet</code> object that will be returned when this <code>PreparedStatement</code> object is executed.
     * <br>
     * Because a <code>PreparedStatement</code> object is precompiled, it is possible to know about the
     * <code>ResultSet</code> object that it will return without having to execute it.  Consequently, it is possible to
     * invoke the method <code>getMetaData</code> on a <code>PreparedStatement</code> object rather than waiting to
     * execute it and then invoking the <code>ResultSet.getMetaData</code> method on the <code>ResultSet</code> object
     * that is returned.
     * <br>
     * <B>NOTE:</B> Using this method may be expensive for some drivers due to the lack of underlying DBMS support.
     *
     * @return the description of a <code>ResultSet</code> object's columns or <code>null</code> if the driver cannot
     * return a <code>ResultSetMetaData</code> object
     * @throws SQLException                    if a database access error occurs or this method is called on a closed
     *                                                  <code>PreparedStatement</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this method
     * @since 1.2
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClose();
        ResultSet rs = getResultSet();
        if (rs != null) {
            return rs.getMetaData();
        }
        if (resultSetMetaData == null) {
            setParametersData();
        }
        return resultSetMetaData;
    }


    protected void setParameter(final int parameterIndex, final ParameterHolder holder) throws SQLException {
        if (parameterIndex >= 1 && parameterIndex  < prepareResult.getParamCount() + 1) {
            parameters[parameterIndex - 1] = holder;
        } else {
            throw ExceptionMapper.getSqlException("Could not set parameter at position " + parameterIndex
                    + " (values vas " + holder.toString() + ")");
        }
    }


    /**
     * Retrieves the number, types and properties of this <code>PreparedStatement</code> object's parameters.
     *
     * @return a <code>ParameterMetaData</code> object that contains information about the number, types and properties
     * for each parameter marker of this <code>PreparedStatement</code> object
     * @throws SQLException if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @see ParameterMetaData
     * @since 1.4
     */
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkClose();
        if (parameterMetaData == null) {
            setParametersData();
        }
        return parameterMetaData;
    }

    private void setParametersData() throws SQLException {
        MariaDbServerPreparedStatement ssps = new MariaDbServerPreparedStatement(connection, this.sqlQuery, ResultSet.TYPE_SCROLL_INSENSITIVE);
        resultSetMetaData = ssps.getMetaData();
        parameterMetaData = ssps.getParameterMetaData();
        ssps.close();
    }


    /**
     * Clears the current parameter values immediately. <P>In general, parameter values remain in force for repeated use
     * of a statement. Setting a parameter value automatically clears its previous value.  However, in some cases it is
     * useful to immediately release the resources used by the current parameter values; this can be done by calling the
     * method <code>clearParameters</code>.
     */
    public void clearParameters() {
        parameters = new ParameterHolder[prepareResult.getParamCount()];
    }


    // Close prepared statement, maybe fire closed-statement events
    @Override
    public void close() throws SQLException {
        super.close();
        if (connection == null || connection.pooledConnection == null
                || connection.pooledConnection.statementEventListeners.isEmpty()) {
            return;
        }
    }

    protected int getParameterCount() {
        return prepareResult.getParamCount();
    }

    /**
     * {inherit}.
     */
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer("sql : '" + sqlQuery + "'");
        sb.append(", parameters : [");
        for (int i = 0; i < parameters.length; i++) {
            ParameterHolder holder = parameters[i];
            if (holder == null) {
                sb.append("null");
            } else {
                sb.append(holder.toString());
            }
            if (i != parameters.length - 1) {
                sb.append(",");
            }
        }
        sb.append("]");
        return sb.toString();
    }


    protected ClientPrepareResult getPrepareResult() {
        return prepareResult;
    }

}
