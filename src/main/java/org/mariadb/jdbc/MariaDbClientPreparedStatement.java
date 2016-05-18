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
import org.mariadb.jdbc.internal.util.dao.QueryException;

import java.sql.*;
import java.util.*;


public class MariaDbClientPreparedStatement extends AbstractMariaDbPrepareStatement implements Cloneable {
    private String sqlQuery;
    private List<String> queryParts;
    private ParameterHolder[] parameters;
    private List<ParameterHolder[]> parameterList = new ArrayList<>();
    private int paramCount;
    private ResultSetMetaData resultSetMetaData = null;
    private ParameterMetaData parameterMetaData = null;
    private boolean reWritablePrepare = true;

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
        this.sqlQuery = sql;
        useFractionalSeconds = connection.getProtocol().getOptions().useFractionalSeconds;
        queryParts = createRewritableParts(sql, connection.noBackslashEscapes);
        paramCount = queryParts.size() - 3;
        parameters = new ParameterHolder[paramCount];
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
        clone.queryParts = queryParts;
        clone.paramCount = paramCount;
        clone.parameters = new ParameterHolder[paramCount];
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
     * @throws java.sql.SQLException if a database access error occurs;
     *                               this method is called on a closed <code>PreparedStatement</code>
     *                               or an argument is supplied to this method
     * @see java.sql.Statement#execute
     * @see java.sql.Statement#getResultSet
     * @see java.sql.Statement#getUpdateCount
     * @see java.sql.Statement#getMoreResults
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
     * @throws java.sql.SQLException if a database access error occurs;
     *                               this method is called on a closed  <code>PreparedStatement</code> or the SQL
     *                               statement does not return a <code>ResultSet</code> object
     */
    public ResultSet executeQuery() throws SQLException {
        if (executeInternal()) {
            return executionResult.getResult();
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
     * @throws java.sql.SQLException if a database access error occurs; this method is called on a closed
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
        lock.lock();
        try {
            executeQueryProlog();
            batchResultSet = null;
            SingleExecutionResult executionResultTmp = new SingleExecutionResult(this, getFetchSize(), true, false);
            protocol.executeQueries(executionResultTmp, queryParts, Collections.singletonList(parameters), resultSetScrollType, false);
            cacheMoreResults(executionResultTmp, getFetchSize(), false);
            executionResult = executionResultTmp;
            return executionResult.getResult() != null;
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
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @see java.sql.Statement#addBatch
     * @since 1.2
     */
    public void addBatch() throws SQLException {
        parameterList.add(parameters);
        clearParameters();
    }

    /**
     * Add batch.
     * @param sql typically this is a SQL <code>INSERT</code> or <code>UPDATE</code> statement
     * @throws java.sql.SQLException every time since that method is forbidden on prepareStatement
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
        this.parameters = new ParameterHolder[paramCount];
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

        int[] ret = new int[size];
        int batchQueriesCount = 0;
        MultiIntExecutionResult internalExecutionResult = new MultiIntExecutionResult(this, size, 0, false);
        lock.lock();
        try {
            QueryException exception = null;
            executeQueryProlog();
            try {
                if (reWritablePrepare && (protocol.getOptions().allowMultiQueries || protocol.getOptions().rewriteBatchedStatements)) {
                    boolean rewrittenBatch = reWritablePrepare && protocol.getOptions().rewriteBatchedStatements;
                    protocol.executeQueries(internalExecutionResult, queryParts, parameterList, resultSetScrollType, rewrittenBatch);
                    cacheMoreResults(internalExecutionResult, getFetchSize(), false);
                    if (rewrittenBatch) {
                        //operation will be done on first execution ( or a few execution if max packet size is not enought for one operation)
                        internalExecutionResult.updateResultsForRewrite();
                    } else {
                        //set update result right (first will have one operation, others are on moreResultPacket).
                        internalExecutionResult.updateResultsMultiple(cachedExecutionResults);
                    }

                } else {
                    for (; batchQueriesCount < size; batchQueriesCount++) {
                        protocol.executeQueries(internalExecutionResult, queryParts, Collections.singletonList(parameterList.get(batchQueriesCount)),
                                resultSetScrollType, false);
                        cacheMoreResults(internalExecutionResult, 0, false);
                    }
                }
            } catch (QueryException e) {
                exception = e;
            } finally {
                executionResult = internalExecutionResult;
                executing = false;
                executeQueryEpilog(exception);
            }

        } catch (SQLException sqle) {
            throw new BatchUpdateException(sqle.getMessage(), sqle.getSQLState(), sqle.getErrorCode(), Arrays.copyOf(ret, batchQueriesCount), sqle);
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
     * @throws java.sql.SQLException                    if a database access error occurs or this method is called on a closed
     *                                                  <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException if the JDBC driver does not support this method
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
        if (parameterIndex >= 1 && parameterIndex  < paramCount + 1) {
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
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @see java.sql.ParameterMetaData
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
        ssps.close();
        resultSetMetaData = ssps.getMetaData();
        parameterMetaData = ssps.getParameterMetaData();
    }


    /**
     * Clears the current parameter values immediately. <P>In general, parameter values remain in force for repeated use
     * of a statement. Setting a parameter value automatically clears its previous value.  However, in some cases it is
     * useful to immediately release the resources used by the current parameter values; this can be done by calling the
     * method <code>clearParameters</code>.
     */
    public void clearParameters() {
        parameters = new ParameterHolder[paramCount];
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
        return paramCount;
    }

    public String toString() {
        return sqlQuery;
    }

    /**
     * Separate query in a String list and set flag reWritablePrepare
     * The parameters "?" (not in comments) emplacements are to be known.
     *
     * The only rewritten queries follow these notation:
     * INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE] [INTO] tbl_name [PARTITION (partition_list)] [(col,...)]
     * {VALUES | VALUE} (...) [ ON DUPLICATE KEY UPDATE col=expr [, col=expr] ... ]
     * With expr without parameter.
     *
     * INSERT ... SELECT will not be rewritten.
     *
     * String list :
     *
     *  - pre value part
     *  - After value and first parameter part
     *  - for each parameters :
     *       - part after parameter and before last parenthesis
     *  - Last query part
     *
     * example : INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ?, 5, ?, 8) ON DUPLICATE KEY UPDATE col2=col2+10
     *
     *  - pre value part : INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES
     *  - after value part : " (9 "
     *  - part after parameter 1: ", 5,"
     *     - ", 5,"
     *     - ",8)"
     *  - last part : ON DUPLICATE KEY UPDATE col2=col2+10
     *
     * With 2 series of parameters, this query will be rewritten like
     * [INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES][ (9, param0_1, 5, param0_2, 8)][, (9, param1_1, 5, param1_2, 8)][ ON DUPLICATE
     * KEY UPDATE col2=col2+10]
     *
     * @param queryString query String
     * @param noBackslashEscapes must backslash be escaped.
     * @return List of query part.
     */
    private List<String> createRewritableParts(String queryString, boolean noBackslashEscapes) {
        reWritablePrepare = true;
        List<String> partList = new ArrayList<>();
        LexState state = LexState.Normal;
        char lastChar = '\0';

        StringBuilder sb = new StringBuilder();

        String preValuePart1 = null;
        String preValuePart2 = null;
        String postValuePart = null;

        boolean singleQuotes = false;

        int isInParenthesis = 0;
        boolean skipChar = false;
        boolean isFirstChar = true;
        boolean isInsert = false;
        boolean semicolon = false;
        boolean hasParam = false;

        char[] query = queryString.toCharArray();

        for (int i = 0; i < query.length; i++) {

            if (state == LexState.Escape) {
                sb.append(query[i]);
                state = LexState.String;
                continue;
            }

            char car = query[i];
            switch (car) {
                case '*':
                    if (state == LexState.Normal && lastChar == '/')  state = LexState.SlashStarComment;
                    break;
                case '/':
                    if (state == LexState.SlashStarComment && lastChar == '*') {
                        state = LexState.Normal;
                    } else if (state == LexState.Normal && lastChar == '/') {
                        state = LexState.EOLComment;
                    }
                    break;

                case '#':
                    if (state == LexState.Normal) state = LexState.EOLComment;
                    break;

                case '-':
                    if (state == LexState.Normal && lastChar == '-') state = LexState.EOLComment;
                    break;

                case '\n':
                    if (state == LexState.EOLComment) state = LexState.Normal;
                    break;

                case '"':
                    if (state == LexState.Normal) {
                        state = LexState.String;
                        singleQuotes = false;
                    } else if (state == LexState.String && !singleQuotes) {
                        state = LexState.Normal;
                    }
                    break;
                case ';':
                    if (state == LexState.Normal) {
                        semicolon = true;
                    }
                    break;
                case '\'':
                    if (state == LexState.Normal) {
                        state = LexState.String;
                        singleQuotes = true;
                    } else if (state == LexState.String && singleQuotes) {
                        state = LexState.Normal;
                    }
                    break;

                case '\\':
                    if (noBackslashEscapes) {
                        break;
                    }
                    if (state == LexState.String) state = LexState.Escape;
                    break;

                case '?':
                    if (state == LexState.Normal) {
                        hasParam = true;
                        if (preValuePart1 == null) {
                            preValuePart1 = sb.toString();
                            sb.setLength(0);
                        }
                        if (preValuePart2 == null) {
                            preValuePart2 = sb.toString();
                            sb.setLength(0);
                        } else {
                            if (postValuePart != null) {
                                //having parameters after the last ")" of value is not rewritable
                                reWritablePrepare = false;

                                //add part
                                sb.insert(0, postValuePart);
                                postValuePart = null;
                            }
                            partList.add(sb.toString());
                            sb.setLength(0);
                        }

                        skipChar = true;
                    }
                    break;
                case '`':
                    if (state == LexState.Backtick) {
                        state = LexState.Normal;
                    } else if (state == LexState.Normal) {
                        state = LexState.Backtick;
                    }
                    break;

                case 's':
                case 'S':
                    if (state == LexState.Normal) {
                        if (postValuePart == null
                                && query.length > i + 6
                                && (query[i + 1] == 'e' || query[i + 1] == 'E')
                                && (query[i + 2] == 'l' || query[i + 2] == 'L')
                                && (query[i + 3] == 'e' || query[i + 3] == 'E')
                                && (query[i + 4] == 'c' || query[i + 4] == 'C')
                                && (query[i + 5] == 't' || query[i + 5] == 'T')) {
                            //SELECT queries, INSERT FROM SELECT not rewritable
                            reWritablePrepare = false;
                        }
                    }
                    break;
                case 'v':
                case 'V':
                    if (state == LexState.Normal) {
                        if (preValuePart1 == null
                                && (lastChar == ')' || ((byte) lastChar <= 40))
                                && query.length > i + 7
                                && (query[i + 1] == 'a' || query[i + 1] == 'A')
                                && (query[i + 2] == 'l' || query[i + 2] == 'L')
                                && (query[i + 3] == 'u' || query[i + 3] == 'U')
                                && (query[i + 4] == 'e' || query[i + 4] == 'E')
                                && (query[i + 5] == 's' || query[i + 5] == 'S')
                                && (query[i + 6] == '(' || ((byte) query[i + 6] <= 40))) {
                            sb.append(car);
                            sb.append(query[i + 1]);
                            sb.append(query[i + 2]);
                            sb.append(query[i + 3]);
                            sb.append(query[i + 4]);
                            sb.append(query[i + 5]);
                            i = i + 5;
                            preValuePart1 = sb.toString();
                            sb.setLength(0);
                            skipChar = true;
                        }
                    }
                    break;
                case '(':
                    if (state == LexState.Normal) isInParenthesis++;
                    break;
                case ')':
                    if (state == LexState.Normal) {
                        isInParenthesis--;
                        if (isInParenthesis == 0 && preValuePart2 != null && postValuePart == null) {
                            sb.append(car);
                            postValuePart = sb.toString();
                            sb.setLength(0);
                            skipChar = true;
                        }
                    }
                    break;
                default:
                    if (state == LexState.Normal && isFirstChar && ((byte) car >= 40)) {
                        if (car == 'I' || car == 'i') isInsert = true;
                        isFirstChar = false;
                    }
                    //multiple queries
                    if (state == LexState.Normal && semicolon && ((byte) car >= 40)) reWritablePrepare = false;
                    break;
            }

            lastChar = car;
            if (skipChar) {
                skipChar = false;
            } else {
                sb.append(car);
            }
        }

        partList.add(0, (preValuePart1 == null) ? "" : preValuePart1);
        if (!hasParam) {
            //permit to have rewrite without parameter
            partList.add(1, sb.toString());
            sb.setLength(0);
        } else partList.add(1, (preValuePart2 == null) ? "" : preValuePart2);

        if (!isInsert) reWritablePrepare = false;

        //postValuePart is the value after the last parameter and parenthesis
        //if no param, don't add to the list.
        if (hasParam) partList.add((postValuePart == null) ? "" : postValuePart);
        partList.add(sb.toString());
        return partList;
    }

    protected List<String> getQueryParts() {
        return queryParts;
    }

    protected int getParamCount() {
        return paramCount;
    }

    public boolean isReWritablePrepare() {
        return reWritablePrepare;
    }
}
