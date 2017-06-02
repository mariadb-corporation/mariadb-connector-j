
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

import org.mariadb.jdbc.internal.com.read.dao.Results;
import org.mariadb.jdbc.internal.com.read.resultset.SelectResultSet;
import org.mariadb.jdbc.internal.com.send.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.util.dao.ClientPrepareResult;
import org.mariadb.jdbc.internal.util.exceptions.ExceptionMapper;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class MariaDbPreparedStatementClient extends BasePrepareStatement {
    private static Logger logger = LoggerFactory.getLogger(MariaDbPreparedStatementClient.class);
    protected ClientPrepareResult prepareResult;
    protected List<ParameterHolder[]> parameterList = new ArrayList<>();
    private String sqlQuery;
    private ParameterHolder[] parameters;
    private ResultSetMetaData resultSetMetaData = null;
    private ParameterMetaData parameterMetaData = null;

    /**
     * Constructor.
     *
     * @param connection          connection
     * @param sql                 sql query
     * @param resultSetScrollType one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                            <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @throws SQLException exception
     */
    public MariaDbPreparedStatementClient(MariaDbConnection connection, String sql, int resultSetScrollType) throws SQLException {
        super(connection, resultSetScrollType);
        this.sqlQuery = sql;

        if (options.cachePrepStmts) {
            String key = new StringBuilder(this.protocol.getDatabase()).append("-").append(sqlQuery).toString();
            prepareResult = connection.getClientPrepareStatementCache().get(key);
        }

        if (prepareResult == null) {
            if (options.rewriteBatchedStatements) {
                prepareResult = ClientPrepareResult.rewritableParts(sqlQuery, connection.noBackslashEscapes);
            } else {
                prepareResult = ClientPrepareResult.parameterParts(sqlQuery, connection.noBackslashEscapes);
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
     * @param connection connection
     * @return Clone statement.
     * @throws CloneNotSupportedException if any error occur.
     */
    public MariaDbPreparedStatementClient clone(MariaDbConnection connection) throws CloneNotSupportedException {
        MariaDbPreparedStatementClient clone = (MariaDbPreparedStatementClient) super.clone(connection);
        clone.sqlQuery = sqlQuery;
        clone.prepareResult = prepareResult;
        clone.parameters = new ParameterHolder[prepareResult.getParamCount()];
        clone.resultSetMetaData = resultSetMetaData;
        clone.parameterMetaData = parameterMetaData;
        return clone;
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
     *                      this method is called on a closed <code>PreparedStatement</code>
     *                      or an argument is supplied to this method
     * @see Statement#execute
     * @see Statement#getResultSet
     * @see Statement#getUpdateCount
     * @see Statement#getMoreResults
     */
    public boolean execute() throws SQLException {
        return executeInternal(getFetchSize());
    }

    /**
     * Executes the SQL query in this <code>PreparedStatement</code> object
     * and returns the <code>ResultSet</code> object generated by the query.
     *
     * @return a <code>ResultSet</code> object that contains the data produced by the
     * query; never <code>null</code>
     * @throws SQLException if a database access error occurs;
     *                      this method is called on a closed  <code>PreparedStatement</code> or the SQL
     *                      statement does not return a <code>ResultSet</code> object
     */
    public ResultSet executeQuery() throws SQLException {
        if (execute()) {
            return results.getResultSet();
        }
        return SelectResultSet.createEmptyResultSet();
    }


    /**
     * Executes the SQL statement in this <code>PreparedStatement</code> object, which must be an SQL Data Manipulation
     * Language (DML) statement, such as <code>INSERT</code>, <code>UPDATE</code> or <code>DELETE</code>; or an SQL
     * statement that returns nothing, such as a DDL statement.
     *
     * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for SQL statements
     * that return nothing
     * @throws SQLException if a database access error occurs; this method is called on a closed
     *                      <code>PreparedStatement</code> or the SQL statement returns a
     *                      <code>ResultSet</code> object
     */
    public int executeUpdate() throws SQLException {
        if (execute()) {
            return 0;
        }
        return getUpdateCount();
    }

    protected boolean executeInternal(int fetchSize) throws SQLException {

        //valid parameters
        for (int i = 0; i < prepareResult.getParamCount(); i++) {
            if (parameters[i] == null) {
                logger.error("Parameter at position " + (i + 1) + " is not set");
                ExceptionMapper.throwException(new SQLException("Parameter at position " + (i + 1) + " is not set", "07004"),
                        connection, this);
            }
        }

        lock.lock();
        try {
            executeQueryPrologue(false);
            results = new Results(this, fetchSize, false, 1, false, resultSetScrollType,
                    protocol.getAutoIncrementIncrement());
            if (queryTimeout != 0 && canUseServerTimeout) {
                //timer will not be used for timeout to avoid having threads
                protocol.executeQuery(protocol.isMasterConnection(), results, prepareResult, parameters, queryTimeout);
            } else {
                protocol.executeQuery(protocol.isMasterConnection(), results, prepareResult, parameters);
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
     * Adds a set of parameters to this <code>PreparedStatement</code> object's batch of send.
     * <br>
     * <br>
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed
     *                      <code>PreparedStatement</code>
     * @see Statement#addBatch
     * @since 1.2
     */
    public void addBatch() throws SQLException {
        ParameterHolder[] holder = new ParameterHolder[prepareResult.getParamCount()];
        for (int i = 0; i < holder.length; i++) {
            holder[i] = parameters[i];
            if (holder[i] == null) {
                logger.error("You need to set exactly " + prepareResult.getParamCount()
                        + " parameters on the prepared statement");
                throw ExceptionMapper.getSqlException("You need to set exactly " + prepareResult.getParamCount()
                        + " parameters on the prepared statement");
            }
        }
        parameterList.add(holder);
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
        if (size == 0) return new int[0];

        boolean rewritten = options.rewriteBatchedStatements && prepareResult.isQueryMultiValuesRewritable();
        lock.lock();
        try {

            executeInternalBatch(size);
            if (!rewritten) {
                return results.getCmdInformation().getUpdateCounts();
            } else {
                return results.getCmdInformation().getRewriteUpdateCounts();
            }

        } catch (SQLException sqle) {
            throw executeBatchExceptionEpilogue(sqle, results.getCmdInformation(), size, rewritten);
        } finally {
            executeBatchEpilogue();
            lock.unlock();
        }
    }

    /**
     * Execute batch, like executeBatch(), with returning results with long[].
     * For when row count may exceed Integer.MAX_VALUE.
     *
     * @return an array of update counts (one element for each command in the batch)
     * @throws SQLException if a database error occur.
     */
    public long[] executeLargeBatch() throws SQLException {
        checkClose();
        int size = parameterList.size();
        if (size == 0) return new long[0];

        boolean rewritten = options.rewriteBatchedStatements && prepareResult.isQueryMultiValuesRewritable();
        lock.lock();
        try {

            executeInternalBatch(size);
            if (!rewritten) {
                return results.getCmdInformation().getLargeUpdateCounts();
            } else {
                return results.getCmdInformation().getRewriteLargeUpdateCounts();
            }

        } catch (SQLException sqle) {
            throw executeBatchExceptionEpilogue(sqle, results.getCmdInformation(), size, rewritten);
        } finally {
            executeBatchEpilogue();
            lock.unlock();
        }
    }

    /**
     * Choose better way to execute queries according to query and options.
     *
     * @param size parameters number
     * @throws SQLException if any error occur
     */
    protected void executeInternalBatch(int size) throws SQLException {

        executeQueryPrologue(true);
        results.reset(0, true, size, false, resultSetScrollType);

        if (options.rewriteBatchedStatements) {
            if (prepareResult.isQueryMultiValuesRewritable()) {
                //values rewritten in one query :
                // INSERT INTO X(a,b) VALUES (1,2), (3,4), ...
                protocol.executeBatchRewrite(protocol.isMasterConnection(), results, prepareResult,
                        parameterList, true);
                return;
            } else if (prepareResult.isQueryMultipleRewritable()) {
                //multi rewritten in one query :
                // INSERT INTO X(a,b) VALUES (1,2);INSERT INTO X(a,b) VALUES (3,4); ...
                protocol.executeBatchRewrite(protocol.isMasterConnection(), results, prepareResult,
                        parameterList, false);
                return;
            }
        }

        if (options.useBatchMultiSend) {
            //send by bulk : send data by bulk before reading corresponding results
            protocol.executeBatchMulti(protocol.isMasterConnection(), results, prepareResult, parameterList);
        } else {
            //send query one by one, reading results for each query before sending another one
            SQLException exception = null;
            for (int batchQueriesCount = 0; batchQueriesCount < size; batchQueriesCount++) {
                try {

                    protocol.executeQuery(protocol.isMasterConnection(), results, prepareResult, parameterList.get(batchQueriesCount));

                } catch (SQLException e) {
                    if (options.continueBatchOnError) {
                        exception = e;
                    } else {
                        throw e;
                    }
                }
            }
            if (exception != null) throw exception;
        }

        results.commandEnd();
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
     *                                         <code>PreparedStatement</code>
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
        if (parameterIndex >= 1 && parameterIndex < prepareResult.getParamCount() + 1) {
            parameters[parameterIndex - 1] = holder;
        } else {
            logger.error("Could not set parameter at position " + parameterIndex
                    + " (values vas " + holder.toString() + ")");

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
     *                      <code>PreparedStatement</code>
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
        MariaDbPreparedStatementServer ssps = new MariaDbPreparedStatementServer(connection, this.sqlQuery,
                ResultSet.TYPE_SCROLL_INSENSITIVE, true);
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
