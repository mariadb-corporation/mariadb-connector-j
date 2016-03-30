package org.mariadb.jdbc.internal.protocol;

import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.packet.result.*;
import org.mariadb.jdbc.internal.packet.send.*;
import org.mariadb.jdbc.internal.queryresults.*;
import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;
import org.mariadb.jdbc.internal.stream.MaxAllowedPacketException;
import org.mariadb.jdbc.internal.util.ExceptionMapper;
import org.mariadb.jdbc.internal.util.PrepareStatementCache;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.util.constant.ServerStatus;
import org.mariadb.jdbc.internal.util.buffer.Buffer;
import org.mariadb.jdbc.internal.packet.read.Packet;
import org.mariadb.jdbc.internal.packet.dao.parameters.LongDataParameterHolder;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;
import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.util.dao.PrepareResult;

import java.io.*;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

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

Copyright (c) 2009-2011, Marcus Eriksson

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

public class AbstractQueryProtocol extends AbstractConnectProtocol implements Protocol {

    private int transactionIsolationLevel = 0;
    private InputStream localInfileInputStream;
    private int maxRows;  /* max rows returned by a statement */

    /**
     * Get a protocol instance.
     *
     * @param urlParser connection URL infos
     * @param lock      the lock for thread synchronisation
     */

    public AbstractQueryProtocol(final UrlParser urlParser, final ReentrantLock lock) {
        super(urlParser, lock);
    }

    /**
     * Hexdump.
     *
     * @param buffer byte array
     * @param offset offset
     * @return String
     */
    public static String hexdump(byte[] buffer, int offset) {
        StringBuffer dump = new StringBuffer();
        if ((buffer.length - offset) > 0) {
            dump.append(String.format("%02x", buffer[offset]));
            for (int i = offset + 1; i < buffer.length; i++) {
                dump.append(String.format("%02x", buffer[i]));
            }
        }
        return dump.toString();
    }


    @Override
    public PrepareResult prepare(String sql, boolean forceNew) throws QueryException {
        lock.lock();
        try {

            if (activeStreamingResult != null) {
                throw new QueryException("There is an open result set on the current connection, which must be "
                        + "closed prior to executing a query");
            }

            checkClose();
            String key = null;
            if (!forceNew && options.cachePrepStmts) {
                key = new StringBuilder(database).append("-").append(sql).toString();
                PrepareResult pr = prepareStatementCache.get(key);
                if (pr != null && pr.incrementShareCounter()) {
                    return pr;
                }
            }

            writer.sendPreparePacket(sql);

            Buffer buffer = packetFetcher.getReusableBuffer();
            byte firstByte = buffer.getByteAt(0);

            if (firstByte == Packet.ERROR) {
                ErrorPacket ep = new ErrorPacket(buffer);
                String message = ep.getMessage();
                throw new QueryException("Error preparing query: " + message, ep.getErrorNumber(), ep.getSqlState());
            }

            if (firstByte == Packet.OK) {
                /* Prepared Statement OK */
                buffer.readByte(); /* skip field count */
                final int statementId = buffer.readInt();
                final int numColumns = buffer.readShort();
                final int numParams = buffer.readShort();
                buffer.readByte(); // reserved
                this.hasWarnings = buffer.readShort() > 0;
                ColumnInformation[] params = new ColumnInformation[numParams];
                if (numParams > 0) {
                    for (int i = 0; i < numParams; i++) {
                        params[i] = new ColumnInformation(packetFetcher.getPacket());
                    }
                    readEofPacket();
                }
                ColumnInformation[] columns = new ColumnInformation[numColumns];
                if (numColumns > 0) {
                    for (int i = 0; i < numColumns; i++) {
                        columns[i] = new ColumnInformation(packetFetcher.getPacket());
                    }
                    readEofPacket();
                }
                PrepareResult prepareResult = new PrepareResult(statementId, columns, params, this);
                if (options.cachePrepStmts && sql != null && sql.length() < options.prepStmtCacheSqlLimit) {
                    PrepareResult cachedPrepareResult = prepareStatementCache.put(key, prepareResult, forceNew);
                    return cachedPrepareResult != null ? cachedPrepareResult : prepareResult;
                }
                return prepareResult;
            } else {
                throw new QueryException("Unexpected packet returned by server, first byte " + firstByte);
            }
        } catch (IOException e) {
            throw new QueryException(e.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean getAutocommit() {
        lock.lock();
        try {
            return ((serverStatus & ServerStatus.AUTOCOMMIT) != 0);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean inTransaction() {
        return ((serverStatus & ServerStatus.IN_TRANSACTION) != 0);
    }


    @Override
    public boolean hasMoreResults() {
        return moreResults;
    }

    public void closeExplicit() {
        this.explicitClosed = true;
        close();
    }

    /**
     * Rollback transaction.
     */
    public void rollback() {
        lock.lock();
        try {
            if (inTransaction()) {
                executeQuery("ROLLBACK");
            }
        } catch (Exception e) {
            /* eat exception */
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void setCatalog(final String database) throws QueryException {
        lock.lock();
        try {
            checkClose();
            final SendChangeDbPacket packet = new SendChangeDbPacket(database);
            packet.send(writer);
            final Buffer buffer = packetFetcher.getReusableBuffer();
            if (buffer.getByteAt(0) == Packet.ERROR) {
                final ErrorPacket ep = new ErrorPacket(buffer);
                throw new QueryException("Could not select database '" + database + "' : " + ep.getMessage(),
                        ep.getErrorNumber(), ep.getSqlState());
            }
            this.database = database;
        } catch (IOException e) {
            throw new QueryException("Could not select database '" + database + "' :" + e.getMessage(),
                    -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(),
                    e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean ping() throws QueryException {
        lock.lock();
        try {
            checkClose();
            final SendPingPacket pingPacket = new SendPingPacket();
            try {
                pingPacket.send(writer);
                Buffer buffer = packetFetcher.getReusableBuffer();
                return buffer.getByteAt(0) == Packet.OK;
            } catch (IOException e) {
                throw new QueryException("Could not ping: " + e.getMessage(), -1,
                        ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
            }
        } finally {
            lock.unlock();
        }
    }

    private void sendLocalFile(ExecutionResult executionResult, String fileName) throws IOException, QueryException {
        // Server request the local file (LOCAL DATA LOCAL INFILE)
        // We do accept general URLs, too. If the localInfileStream is
        // set, use that.
        int seq = 2;
        InputStream is;
        if (localInfileInputStream == null) {
            if (!getUrlParser().getOptions().allowLocalInfile) {
                writer.writeEmptyPacket(seq++);
                throw new QueryException(
                        "Usage of LOCAL INFILE is disabled. To use it enable it via the connection property allowLocalInfile=true",
                        -1,
                        ExceptionMapper.SqlStates.FEATURE_NOT_SUPPORTED.getSqlState());
            }

            try {
                URL url = new URL(fileName);
                is = url.openStream();
            } catch (IOException ioe) {
                try {
                    is = new FileInputStream(fileName);
                } catch (FileNotFoundException f) {
                    writer.writeEmptyPacket(seq++);
                    packetFetcher.getReusableBuffer();
                    throw new QueryException("Could not send file : " + f.getMessage(), -1, "22000", f);
                }
            }
        } else {
            is = localInfileInputStream;
            localInfileInputStream = null;
        }
        writer.sendFile(is, seq);
        is.close();
        getResult(executionResult, ResultSet.TYPE_FORWARD_ONLY, false);
    }

    @Override
    public void executePreparedQueryAfterFailover(PrepareResult oldPrepareResult, ExecutionResult executionResult, String sql,
                                                  ParameterHolder[] parameters, MariaDbType[] parameterTypeHeader, int resultSetScrollType)
            throws QueryException {
        PrepareResult prepareResult = prepare(sql, true);
        //reset header status
        for (int i = 0; i < parameterTypeHeader.length; i++) {
            parameterTypeHeader[i] = null;
        }
        oldPrepareResult.failover(prepareResult.getStatementId(), this);
        executePreparedQuery(oldPrepareResult, executionResult, sql, parameters, parameterTypeHeader, resultSetScrollType);
    }

    @Override
    public void executePreparedQuery(PrepareResult prepareResult, ExecutionResult executionResult, String sql, ParameterHolder[] parameters,
                                                    MariaDbType[] parameterTypeHeader, int resultSetScrollType) throws QueryException {
        checkClose();
        this.moreResults = false;
        try {
            int parameterCount = parameters.length;
            //send binary data in a separate stream
            for (int i = 0; i < parameterCount; i++) {
                if (parameters[i].isLongData()) {

                    writer.startPacket(0);
                    writer.buffer.put((byte) 0x18);
                    writer.buffer.putInt(prepareResult.getStatementId());
                    writer.buffer.putShort((short) i);
                    ((LongDataParameterHolder) parameters[i]).writeBinary(writer);
                    writer.finishPacket();
                }
            }
            //send execute query
            SendExecutePrepareStatementPacket packet = new SendExecutePrepareStatementPacket(prepareResult.getStatementId(), parameters,
                    parameterCount, parameterTypeHeader);
            packet.send(writer);
            getResult(executionResult, resultSetScrollType, true);

        } catch (QueryException qex) {
            if (sql.length() > 1024) {
                qex.setMessage(qex.getMessage() + "\nQuery is: " + sql.substring(0, 1024));
            } else {
                qex.setMessage(qex.getMessage() + "\nQuery is: " + sql);
            }
            if (qex.getCause() instanceof SocketTimeoutException) {
                throw new QueryException("Connection timed out", -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), qex);
            } else {
                throw qex;
            }
        } catch (MaxAllowedPacketException e) {
            if (e.isMustReconnect()) {
                connect();
            }
            throw new QueryException("Could not send query: " + e.getMessage(), -1,
                    ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION.getSqlState(), e);
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    /**
     * Deallocate prepare statement if not used anymore.
     * @param prepareResult allocation result
     * @param sql sql query
     * @throws QueryException if deallocation failed.
     */
    @Override
    public void releasePrepareStatement(PrepareResult prepareResult, String sql) throws QueryException {
        //If prepared cache is enable, the PrepareResult can be shared in many PrepStatement, so synchronised use count indicator will be decrement.
        prepareResult.decrementShareCounter();

        //deallocate from server if not cached
        if (prepareResult.canBeDeallocate()) {
            forceReleasePrepareStatement(prepareResult.getStatementId());
        }
    }

    /**
     * Force release of prepare statement that are not used.
     * This method will be call when adding a new preparestatement in cache, so the packet can be send to server without
     * problem.
     *
     * @param statementId prepared statement Id to remove.
     * @throws QueryException if connection exception.
     */
    public void forceReleasePrepareStatement(int statementId) throws QueryException {
        lock.lock();
        try {
            checkClose();
            final SendClosePrepareStatementPacket packet = new SendClosePrepareStatementPacket(statementId);
            try {
                packet.send(writer);
            } catch (IOException e) {
                throw new QueryException("Could not send query: " + e.getMessage(), -1,
                        ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Cancels the current query - clones the current protocol and executes a query using the new connection.
     *
     * @throws QueryException never thrown
     * @throws IOException    if Host is not responding
     */
    @Override
    public void cancelCurrentQuery() throws QueryException, IOException {
        MasterProtocol copiedProtocol = new MasterProtocol(urlParser, new ReentrantLock());
        copiedProtocol.setHostAddress(getHostAddress());
        copiedProtocol.connect();
        //no lock, because there is already a query running that possessed the lock.
        copiedProtocol.executeQuery("KILL QUERY " + serverThreadId);
        copiedProtocol.close();
    }

    @Override
    public void getMoreResults(ExecutionResult executionResult) throws QueryException {
        if (!hasMoreResults()) {
            return;
        }
        getResult(executionResult, ResultSet.TYPE_FORWARD_ONLY,
                (activeStreamingResult != null) ? activeStreamingResult.isBinaryEncoded() : moreResultsTypeBinary);
    }

    /**
     * Set max row retuen by a statement.
     *
     * @param max row number max value
     */
    public void setInternalMaxRows(int max) {
        if (maxRows != max) {
            maxRows = max;
        }
    }

    public int getMaxRows() {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws QueryException {
        if (maxRows != max) {
            if (max == 0) {
                executeQuery("set @@SQL_SELECT_LIMIT=DEFAULT");
            } else {
                executeQuery("set @@SQL_SELECT_LIMIT=" + max);
            }
            maxRows = max;
        }
    }


    @Override
    public void setLocalInfileInputStream(InputStream inputStream) {
        this.localInfileInputStream = inputStream;
    }

    /**
     * Returns the connection timeout in milliseconds.
     *
     * @return the connection timeout in milliseconds.
     * @throws SocketException if there is an error in the underlying protocol, such as a TCP error.
     */
    @Override
    public int getTimeout() throws SocketException {
        return this.socket.getSoTimeout();
    }

    /**
     * Sets the connection timeout.
     *
     * @param timeout the timeout, in milliseconds
     * @throws SocketException if there is an error in the underlying protocol, such as a TCP error.
     */
    @Override
    public void setTimeout(int timeout) throws SocketException {
        lock.lock();
        try {
            this.getOptions().socketTimeout = timeout;
            this.socket.setSoTimeout(timeout);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Set transaction isolation.
     *
     * @param level transaction level.
     * @throws QueryException if transaction level is unknown
     */
    public void setTransactionIsolation(final int level) throws QueryException {
        lock.lock();
        try {
            String query = "SET SESSION TRANSACTION ISOLATION LEVEL";
            switch (level) {
                case Connection.TRANSACTION_READ_UNCOMMITTED:
                    query += " READ UNCOMMITTED";
                    break;
                case Connection.TRANSACTION_READ_COMMITTED:
                    query += " READ COMMITTED";
                    break;
                case Connection.TRANSACTION_REPEATABLE_READ:
                    query += " REPEATABLE READ";
                    break;
                case Connection.TRANSACTION_SERIALIZABLE:
                    query += " SERIALIZABLE";
                    break;
                default:
                    throw new QueryException("Unsupported transaction isolation level");
            }
            executeQuery(query);
            transactionIsolationLevel = level;
        } finally {
            lock.unlock();
        }
    }

    public int getTransactionIsolationLevel() {
        return transactionIsolationLevel;
    }

    private void checkClose() throws QueryException {
        if (!this.connected) {
            throw new QueryException("Connection is close", 1220, "08000");
        }
    }

    /**
     * Close active result.
     * @throws SQLException if socket error.
     */
    public void fetchActiveStreamingResult() throws SQLException {
        if (activeStreamingResult != null) {
            activeStreamingResult.fetchAllStreaming();
        }
    }

    public PrepareStatementCache prepareStatementCache() {
        return prepareStatementCache;
    }

    public void executeQuery(final String sql) throws QueryException {
        executeQuery(new SingleExecutionResult(null, 0, false, false), sql, ResultSet.TYPE_FORWARD_ONLY);
    }

    /**
     * Execute query.
     *
     * @param sql the query to executeInternal
     * @param resultSetScrollType resultSetScrollType
     * @throws QueryException exception
     */
    @Override
    public void executeQuery(ExecutionResult executionResult, final String sql, int resultSetScrollType) throws QueryException {
        checkClose();
        try {
            writer.sendTextPacket(sql);
            getResult(executionResult, resultSetScrollType, false);
        } catch (QueryException queryException) {
            if (getOptions().dumpQueriesOnException || queryException.getErrorCode() == 1064) {
                String sqlQuery = sql;
                if (sqlQuery.length() > 1024) {
                    sqlQuery = sqlQuery.substring(0, 1024);
                }
                queryException.setMessage(queryException.getMessage() + "\nQuery is : " + sqlQuery);
            }
            throw queryException;
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }

    }

    /**
     * Execute list of queries not rewritable.
     *
     * @param queries list of queryes
     * @param resultSetScrollType resultSetScrollType
     * @throws QueryException exception
     */
    public void executeQueries(ExecutionResult executionResult, List<String> queries, int resultSetScrollType)
            throws QueryException {
        checkClose();
        int counter = 0;
        int size = queries.size();
        String sql = null;
        QueryException exception = null;
        for (; counter < size; counter++) {
            try {
                sql = queries.get(counter);
                writer.sendTextPacket(sql);
                getResult(executionResult, resultSetScrollType, false);
            } catch (QueryException queryException) {
                if (getOptions().dumpQueriesOnException || queryException.getErrorCode() == 1064) {
                    addQueryInfo(sql, queryException);
                }
                if (getOptions().continueBatchOnError) {
                    if (exception == null) {
                        exception = queryException;
                    }
                } else {
                    throw queryException;
                }
            } catch (IOException e) {
                for (int i = 0; i < counter; i++) {
                    queries.remove(0);
                }
                throw new QueryException("Could not send query: " + e.getMessage(), -1,
                        ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Specific execution for batch rewrite that has specific query for memory.
     * @param executionResult result
     * @param queryParts query part
     * @param parameterList parameters
     * @param resultSetScrollType resultsetScroll type
     * @param isRewritable is rewritable flag
     * @throws QueryException exception
     */
    public void executeQueries(ExecutionResult executionResult, final List<String> queryParts, List<ParameterHolder[]> parameterList,
                               int resultSetScrollType, boolean isRewritable) throws QueryException {
        checkClose();
        ParameterHolder[] parameters = null;
        int paramCount = queryParts.size() - 3;
        int currentIndex = 0;
        int totalParameterList = parameterList.size();

        try {
            //validate parameters
            for (ParameterHolder[] parameterHolders : parameterList) {
                for (ParameterHolder ph : parameterHolders) {
                    if (ph == null) {
                        parameters = parameterHolders;
                        throw new QueryException("You need to set exactly " + paramCount + " parameters on the prepared statement");
                    }
                }
            }

            parameters = parameterList.get(currentIndex++);

            //change rewritable part to utf8 bytes
            List<byte[]> queryPartsUtf8 = new ArrayList<>(queryParts.size());
            for (String part : queryParts) {
                queryPartsUtf8.add(part.getBytes("UTF-8"));
            }

            this.moreResults = false;

            do {
                writer.startPacket(0);
                writer.write(0x03);

                if (totalParameterList == 1) {
                    writer.write(queryPartsUtf8.get(0));
                    writer.write(queryPartsUtf8.get(1));
                    for (int i = 0; i < paramCount; i++) {
                        parameters[i].writeTo(writer);
                        writer.write(queryPartsUtf8.get(i + 2));
                    }
                    writer.write(queryPartsUtf8.get(paramCount + 2));
                } else {

                    if (!isRewritable) {
                        //write first
                        writer.write(queryPartsUtf8.get(0));
                        writer.write(queryPartsUtf8.get(1));

                        for (int i = 0; i < paramCount; i++) {
                            parameters[i].writeTo(writer);
                            writer.write(queryPartsUtf8.get(i + 2));
                        }
                        writer.write(queryPartsUtf8.get(paramCount + 2));

                        // write other, separate by ";"
                        while (currentIndex < totalParameterList) {
                            parameters = parameterList.get(currentIndex++);
                            writer.write(';');
                            writer.write(queryPartsUtf8.get(0));
                            writer.write(queryPartsUtf8.get(1));
                            for (int i = 0; i < paramCount; i++) {
                                parameters[i].writeTo(writer);
                                writer.write(queryPartsUtf8.get(i + 2));
                            }
                            writer.write(queryPartsUtf8.get(paramCount + 2));
                        }

                    } else {
                        writer.write(queryPartsUtf8.get(0));
                        writer.write(queryPartsUtf8.get(1));
                        int lastPartLength = queryPartsUtf8.get(paramCount + 2).length;

                        for (int i = 0; i < paramCount; i++) {
                            parameters[i].writeTo(writer);
                            writer.write(queryPartsUtf8.get(i + 2));
                        }

                        while (currentIndex < totalParameterList) {
                            parameters = parameterList.get(currentIndex);

                            //check packet length so to separate in multiple packet
                            int parameterLength = 1;
                            for (ParameterHolder parameter : parameters) {
                                parameterLength += parameter.getApproximateTextProtocolLength();
                            }

                            if (writer.checkRewritableLength(parameterLength + lastPartLength)) {
                                writer.write((byte) 44); //","
                                writer.write(queryPartsUtf8.get(1));

                                for (int i = 0; i < paramCount; i++) {
                                    parameters[i].writeTo(writer);
                                    writer.write(queryPartsUtf8.get(i + 2));
                                }
                                currentIndex++;
                            } else {
                                break;
                            }
                        }
                        writer.write(queryPartsUtf8.get(paramCount + 2));
                    }
                }

                writer.finishPacket();
                getResult(executionResult, resultSetScrollType, false);
            } while (totalParameterList < totalParameterList);

        } catch (QueryException queryException) {
            if (getOptions().dumpQueriesOnException || queryException.getErrorCode() == 1064) {
                StringBuilder queryString = new StringBuilder(queryParts.get(0)).append(queryParts.get(1));
                for (int i = 0; i < paramCount; i++) {
                    if (parameters != null && parameters.length > i) {
                        queryString.append(parameters[i]).append(queryParts.get(i + 2));
                    } else {
                        queryString.append("?").append(queryParts.get(i + 2));
                    }
                }
                queryString.append(queryParts.get(paramCount + 2));
                addQueryInfo(queryString.toString(), queryException);
            }
            throw queryException;
        } catch (MaxAllowedPacketException e) {
            if (e.isMustReconnect()) {
                connect();
            }
            throw new QueryException("Could not send query: " + e.getMessage(), -1, ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION.getSqlState(), e);
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    /**
     * Execute list of queries.
     * This method is used when using text batch statement and using rewriting (allowMultiQueries || rewriteBatchedStatements).
     * queries will be send to server according to max_allowed_packet size.
     *
     * @param queries list of queryes
     * @param resultSetScrollType resultSetScrollType
     * @param isRewritable is rewritable flag
     * @param rewriteOffset rewrite offset
     * @throws QueryException exception
     */
    public void executeQueriesRewrite(ExecutionResult executionResult, List<String> queries, int resultSetScrollType, boolean isRewritable,
                                      int rewriteOffset)
            throws QueryException {
        this.moreResults = false;
        String firstSql = null;
        int currentIndex = 0;
        int totalQueries = queries.size();
        QueryException exception = null;
        do {
            try {
                String sql = queries.get(currentIndex++);
                firstSql = sql;
                if (totalQueries == 1) {
                    writer.sendTextPacket(sql);
                } else {
                    writer.startPacket(0);
                    writer.write(0x03);

                    if (!isRewritable) {
                        //add query with ";"
                        writer.write(sql.getBytes("UTF-8"));

                        while (currentIndex < totalQueries) {
                            byte[] sqlByte = queries.get(currentIndex++).getBytes("UTF-8");
                            if (!writer.checkRewritableLength(sqlByte.length)) {
                                break;
                            }
                            writer.write(';');
                            writer.write(sqlByte);
                        }
                    } else {
                        writer.write(sql.getBytes("UTF-8"));
                        while (currentIndex < totalQueries) {
                            byte[] sqlByte = queries.get(currentIndex).substring(rewriteOffset).getBytes("UTF-8");
                            if (writer.checkRewritableLength(1 + sqlByte.length)) {
                                writer.write(',');
                                writer.write(sqlByte);
                                currentIndex++;
                            } else {
                                break;
                            }
                        }
                    }

                    writer.finishPacket();
                    getResult(executionResult, resultSetScrollType, false);
                }
            } catch (QueryException queryException) {
                if (getOptions().dumpQueriesOnException || queryException.getErrorCode() == 1064) {
                    addQueryInfo(firstSql, queryException);
                }
                if (getOptions().continueBatchOnError) {
                    if (exception == null) {
                        exception = queryException;
                    }
                } else {
                    throw queryException;
                }
            } catch (MaxAllowedPacketException e) {
                if (e.isMustReconnect()) {
                    connect();
                }
                throw new QueryException("Could not send query: " + e.getMessage(), -1,
                        ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION.getSqlState(), e);
            } catch (IOException e) {
                throw new QueryException("Could not send query: " + e.getMessage(), -1,
                        ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
            }
        } while (currentIndex < totalQueries);

        if (exception != null) {
            throw exception;
        }
    }

    private void addQueryInfo(String sql, QueryException queryException) {
        if (sql.length() > 1024) {
            sql = sql.substring(0, 1024);
        }
        queryException.setMessage(queryException.getMessage() + "\nQuery is : " + sql);
    }

    @Override
    public void getResult(ExecutionResult executionResult, int resultSetScrollType, boolean binaryProtocol) throws QueryException {
        Buffer buffer;
        try {
            buffer = packetFetcher.getReusableBuffer();
        } catch (IOException e) {
            try {
                if (writer != null) {
                    writer.writeEmptyPacket(packetFetcher.getLastPacketSeq() + 1);
                    packetFetcher.getReusableBuffer();
                }
            } catch (IOException ee) { }
            throw new QueryException("Could not read resultset: " + e.getMessage(), -1,
                    ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
        switch (buffer.getByteAt(0)) {
            case Packet.OK:
                //OK packet
                buffer.skipByte(); //fieldCount
                final long affectedRows = buffer.getLengthEncodedBinary();
                final long insertId = buffer.getLengthEncodedBinary();
                serverStatus = buffer.readShort();
                this.hasWarnings = (buffer.readShort() > 0);
                this.moreResults = ((serverStatus & ServerStatus.MORE_RESULTS_EXISTS) != 0);
                executionResult.addStats(affectedRows, insertId, hasMoreResults());
                break;
            case Packet.ERROR:
                //Error packet
                this.moreResults = false;
                this.hasWarnings = false;
                buffer.skipByte();
                int errorNumber = buffer.readShort();
                String message;
                String sqlState;
                if (buffer.readByte() == '#') {
                    sqlState = new String(buffer.readRawBytes(5));
                    message = buffer.readString(StandardCharsets.UTF_8);
                } else {
                    // Pre-4.1 message, still can be output in newer versions (e.g with 'Too many connections')
                    message = new String(buffer.buf, StandardCharsets.UTF_8);
                    sqlState = "HY000";
                }
                executionResult.addStats(Statement.EXECUTE_FAILED, Statement.SUCCESS_NO_INFO, hasMoreResults());
                throw new QueryException(message, errorNumber, sqlState);

            case Packet.LOCAL_INFILE:
                //Send fileName
                buffer.getLengthEncodedBinary(); //field count
                String fileName = buffer.readString(StandardCharsets.UTF_8);
                try {
                    sendLocalFile(executionResult, fileName);
                } catch (IOException e) {
                    try {
                        if (writer != null) {
                            writer.writeEmptyPacket(packetFetcher.getLastPacketSeq() + 1);
                            packetFetcher.getReusableBuffer();
                        }
                    } catch (IOException ee) { }
                    throw new QueryException("Could not read resultset: " + e.getMessage(), -1,
                            ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), e);
                }
                break;
            case Packet.EOF:
                if (buffer.remaining() < 9) {
                    throw new QueryException("Could not parse result", (short) -1, ExceptionMapper.SqlStates.INTERRUPTED_EXCEPTION.getSqlState());
                }

            default:
                this.hasWarnings = false;
                long fieldCount = buffer.getLengthEncodedBinary();

                try {
                    boolean callableResult = false;
                    ColumnInformation[] ci = new ColumnInformation[(int) fieldCount];
                    for (int i = 0; i < fieldCount; i++) {
                        ci[i] = new ColumnInformation(packetFetcher.getPacket());
                    }

                    Buffer bufferEof = packetFetcher.getReusableBuffer();
                    if (bufferEof.getByteAt(0) != Packet.EOF) {
                        throw new QueryException("Packets out of order when reading field packets, expected was EOF stream. "
                                + "Packet contents (hex) = " + MasterProtocol.hexdump(bufferEof.buf, 0));
                    } else if (executionResult.isCanHaveCallableResultset()) {
                        EndOfFilePacket endOfFilePacket = new EndOfFilePacket(bufferEof);
                        callableResult = (endOfFilePacket.getStatusFlags() & ServerStatus.PS_OUT_PARAMETERS) != 0;
                    }

                    MariaSelectResultSet mariaSelectResultset = new MariaSelectResultSet(ci, executionResult.getStatement(), this, packetFetcher,
                            binaryProtocol, resultSetScrollType, executionResult.getFetchSize(), callableResult);
                    mariaSelectResultset.initFetch();
                    if (!executionResult.isSelectPossible()) {
                        throw new QueryException("Select command are not permitted via executeBatch() command");
                    }
                    executionResult.addResult(mariaSelectResultset, hasMoreResults());


                } catch (IOException e) {
                    throw new QueryException("Could not read result set: " + e.getMessage(),
                            -1,
                            ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(),
                            e);
                }
                break;

        }

    }

    public void prologProxy(PrepareResult prepareResult, ExecutionResult executionResult, int maxRows, boolean hasProxy,
                            MariaDbConnection connection, Statement statement) throws SQLException {
        prolog(executionResult, maxRows, hasProxy, connection, statement);
    }

    /**
     * Preparation before command.
     * @param executionResult result
     * @param maxRows query max rows
     * @param hasProxy has proxy
     * @param connection current connection
     * @param statement current statement
     * @throws SQLException if any error occur.
     */
    public void prolog(ExecutionResult executionResult, int maxRows, boolean hasProxy, MariaDbConnection connection, Statement statement)
            throws SQLException {
        if (explicitClosed) {
            throw new SQLException("execute() is called on closed connection");
        }
        //old failover handling
        if (!hasProxy) {
            if (shouldReconnectWithoutProxy()) {
                try {
                    connectWithoutProxy();
                } catch (QueryException qe) {
                    ExceptionMapper.throwException(qe, connection, statement);
                }
            }
        }

        try {
            setMaxRows(maxRows);
            fetchActiveStreamingResult();
            if (hasMoreResults()) {
                getMoreResults(executionResult);
            }
        } catch (QueryException qe) {
            ExceptionMapper.throwException(qe, connection, statement);
        }

        connection.reenableWarnings();
    }

}
