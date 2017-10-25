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

package org.mariadb.jdbc.internal.protocol;

import org.mariadb.jdbc.LocalInfileInterceptor;
import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.MariaDbStatement;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.MariaDbServerCapabilities;
import org.mariadb.jdbc.internal.com.read.Buffer;
import org.mariadb.jdbc.internal.com.read.ErrorPacket;
import org.mariadb.jdbc.internal.com.read.dao.Results;
import org.mariadb.jdbc.internal.com.read.resultset.ColumnInformation;
import org.mariadb.jdbc.internal.com.read.resultset.SelectResultSet;
import org.mariadb.jdbc.internal.com.read.resultset.UpdatableResultSet;
import org.mariadb.jdbc.internal.com.send.ComQuery;
import org.mariadb.jdbc.internal.com.send.ComStmtExecute;
import org.mariadb.jdbc.internal.com.send.ComStmtPrepare;
import org.mariadb.jdbc.internal.com.send.SendChangeDbPacket;
import org.mariadb.jdbc.internal.com.send.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;
import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.util.BulkStatus;
import org.mariadb.jdbc.internal.util.LogQueryTool;
import org.mariadb.jdbc.internal.util.Utils;
import org.mariadb.jdbc.internal.util.constant.ServerStatus;
import org.mariadb.jdbc.internal.util.constant.StateChange;
import org.mariadb.jdbc.internal.util.dao.ClientPrepareResult;
import org.mariadb.jdbc.internal.util.dao.PrepareResult;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;
import org.mariadb.jdbc.internal.util.exceptions.ExceptionMapper;
import org.mariadb.jdbc.internal.util.exceptions.MaxAllowedPacketException;
import org.mariadb.jdbc.internal.util.pool.GlobalStateInfo;
import org.mariadb.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;

import static org.mariadb.jdbc.internal.com.Packet.*;
import static org.mariadb.jdbc.internal.util.SqlStates.*;


public class AbstractQueryProtocol extends AbstractConnectProtocol implements Protocol {
    protected static ThreadPoolExecutor readScheduler = null;
    private static final Logger logger = LoggerFactory.getLogger(AbstractQueryProtocol.class);
    private int transactionIsolationLevel = 0;
    private InputStream localInfileInputStream;
    private long maxRows;  /* max rows returned by a statement */
    private volatile int statementIdToRelease = -1;
    private FutureTask activeFutureTask = null;
    private final LogQueryTool logQuery;
    private boolean interrupted;

    /**
     * Get a protocol instance.
     *
     * @param urlParser connection URL information's
     * @param lock      the lock for thread synchronisation
     */

    AbstractQueryProtocol(final UrlParser urlParser, final GlobalStateInfo globalInfo, final ReentrantLock lock) {
        super(urlParser, globalInfo, lock);
        logQuery = new LogQueryTool(options);
    }

    /**
     * Reset connection state.
     *
     * <ol>
     *     <li>Transaction will be rollback</li>
     *     <li>transaction isolation will be reset</li>
     *     <li>user variables will be removed</li>
     *     <li>sessions variables will be reset to global values</li>
     * </ol>
     *
     * @throws SQLException if command failed
     */
    @Override
    public void reset() throws SQLException {
        cmdPrologue();
        try {

            writer.startPacket(0);
            writer.write(COM_RESET_CONNECTION);
            writer.flush();
            getResult(new Results());

        } catch (SQLException sqlException) {
            throw logQuery.exceptionWithQuery("COM_RESET_CONNECTION failed.", sqlException, explicitClosed);
        } catch (IOException e) {
            throw handleIoException(e);
        }

    }

    /**
     * Execute internal query.
     * <p>
     * !! will not support multi values queries !!
     *
     * @param sql sql
     * @throws SQLException in any exception occur
     */
    public void executeQuery(final String sql) throws SQLException {
        executeQuery(isMasterConnection(), new Results(), sql);
    }

    /**
     * Execute query directly to outputStream.
     *
     * @param mustExecuteOnMaster was intended to be launched on master connection
     * @param results             result
     * @param sql                 the query to executeInternal
     * @throws SQLException exception
     */
    @Override
    public void executeQuery(boolean mustExecuteOnMaster, Results results, final String sql) throws SQLException {

        cmdPrologue();
        try {

            writer.startPacket(0);
            writer.write(COM_QUERY);
            writer.write(sql);
            writer.flush();
            getResult(results);

        } catch (SQLException sqlException) {
            throw logQuery.exceptionWithQuery(sql, sqlException, explicitClosed);
        } catch (IOException e) {
            throw handleIoException(e);
        }

    }

    @Override
    public void executeQuery(boolean mustExecuteOnMaster, Results results, final String sql, Charset charset) throws SQLException {
        cmdPrologue();
        try {

            writer.startPacket(0);
            writer.write(COM_QUERY);
            writer.write(sql.getBytes(charset));
            writer.flush();
            getResult(results);

        } catch (SQLException sqlException) {
            throw logQuery.exceptionWithQuery(sql, sqlException, explicitClosed);
        } catch (IOException e) {
            throw handleIoException(e);
        }

    }


    /**
     * Execute a unique clientPrepareQuery.
     *
     * @param mustExecuteOnMaster was intended to be launched on master connection
     * @param results             results
     * @param clientPrepareResult clientPrepareResult
     * @param parameters          parameters
     * @throws SQLException exception
     */
    public void executeQuery(boolean mustExecuteOnMaster, Results results, final ClientPrepareResult clientPrepareResult,
                             ParameterHolder[] parameters) throws SQLException {
        cmdPrologue();
        try {

            if (clientPrepareResult.getParamCount() == 0 && !clientPrepareResult.isQueryMultiValuesRewritable()) {
                if (clientPrepareResult.getQueryParts().size() == 1) {
                    ComQuery.sendDirect(writer, clientPrepareResult.getQueryParts().get(0));
                } else {
                    ComQuery.sendMultiDirect(writer, clientPrepareResult.getQueryParts());
                }
            } else {
                writer.startPacket(0);
                ComQuery.sendSubCmd(writer, clientPrepareResult, parameters);
                writer.flush();
            }
            getResult(results);

        } catch (SQLException queryException) {
            throw logQuery.exceptionWithQuery(parameters, queryException, clientPrepareResult);
        } catch (IOException e) {
            throw handleIoException(e);
        }
    }

    /**
     * Execute a unique clientPrepareQuery.
     *
     * @param mustExecuteOnMaster was intended to be launched on master connection
     * @param results             results
     * @param clientPrepareResult clientPrepareResult
     * @param parameters          parameters
     * @param queryTimeout        if timeout is set and must use max_statement_time
     * @throws SQLException exception
     */
    public void executeQuery(boolean mustExecuteOnMaster, Results results, final ClientPrepareResult clientPrepareResult,
                             ParameterHolder[] parameters, int queryTimeout) throws SQLException {
        cmdPrologue();
        try {

            if (clientPrepareResult.getParamCount() == 0 && !clientPrepareResult.isQueryMultiValuesRewritable()) {
                if (clientPrepareResult.getQueryParts().size() == 1) {
                    ComQuery.sendDirect(writer, clientPrepareResult.getQueryParts().get(0), queryTimeout);
                } else {
                    ComQuery.sendMultiDirect(writer, clientPrepareResult.getQueryParts(), queryTimeout);
                }
            } else {
                writer.startPacket(0);
                ComQuery.sendSubCmd(writer, clientPrepareResult, parameters);
                writer.flush();
            }
            getResult(results);

        } catch (SQLException queryException) {
            throw logQuery.exceptionWithQuery(parameters, queryException, clientPrepareResult);
        } catch (IOException e) {
            throw handleIoException(e);
        }
    }

    /**
     * Execute clientPrepareQuery batch.
     *
     * @param mustExecuteOnMaster   was intended to be launched on master connection
     * @param results               results
     * @param prepareResult         ClientPrepareResult
     * @param parametersList        List of parameters
     * @param hasLongData           has parameter with long data (stream)
     * @throws SQLException exception
     */
    public boolean executeBatchClient(boolean mustExecuteOnMaster, Results results, final ClientPrepareResult prepareResult,
                                      final List<ParameterHolder[]> parametersList, boolean hasLongData) throws SQLException {

        //***********************************************************************************************************
        // Multiple solution for batching :
        // - rewrite as multi-values (only if generated keys are not needed and query can be rewritten)
        // - multiple INSERT separate by semi-columns
        // - use pipeline
        // - use bulk
        // - one after the other
        //***********************************************************************************************************

        if (options.rewriteBatchedStatements) {
            if (prepareResult.isQueryMultiValuesRewritable() && results.getAutoGeneratedKeys() == Statement.NO_GENERATED_KEYS) {

                //values rewritten in one query :
                // INSERT INTO X(a,b) VALUES (1,2), (3,4), ...
                executeBatchRewrite(results, prepareResult, parametersList, true);
                return true;

            } else if (prepareResult.isQueryMultipleRewritable()) {

                if (options.useBulkStmts
                        && !hasLongData
                        && results.getAutoGeneratedKeys() == Statement.NO_GENERATED_KEYS
                        && versionGreaterOrEqual(10,2,7)
                        && executeBulkBatch(results, prepareResult.getSql(), null, parametersList)) {
                    return true;
                }

                //multi rewritten in one query :
                // INSERT INTO X(a,b) VALUES (1,2);INSERT INTO X(a,b) VALUES (3,4); ...
                executeBatchRewrite(results, prepareResult, parametersList, false);
                return true;
            }
        }

        if (options.useBulkStmts
                && !hasLongData
                && results.getAutoGeneratedKeys() == Statement.NO_GENERATED_KEYS
                && versionGreaterOrEqual(10, 2, 7)
                && executeBulkBatch(results, prepareResult.getSql(), null, parametersList)) {
            return true;
        }

        if (options.useBatchMultiSend) {
            //send by bulk : send data by bulk before reading corresponding results
            executeBatchMulti(results, prepareResult, parametersList);
            return true;
        }

        return false;
    }

    /**
     * Execute clientPrepareQuery batch.
     *
     * @param results               results
     * @param sql                   sql command
     * @param serverPrepareResult   prepare result if exist
     * @param parametersList        List of parameters
     * @return if executed
     * @throws SQLException exception
     */
    private boolean executeBulkBatch(Results results, String sql, ServerPrepareResult serverPrepareResult,
                                  final List<ParameterHolder[]> parametersList) throws SQLException {

        //**************************************************************************************
        // Ensure BULK can be use :
        // - server version >= 10.2.7
        // - no stream
        // - parameter type doesn't change
        //**************************************************************************************

        //ensure that there is no long data and type doesn't change
        ParameterHolder[] initParameters = parametersList.get(0);
        int parameterCount = initParameters.length;
        short[] types = new short[parameterCount];
        for (int i = 0; i < parameterCount; i++) {
            types[i] = initParameters[i].getColumnType().getType();
        }

        //must ensure that data type doesn't change
        for (ParameterHolder[] parameters : parametersList) {
            for (int i = 0; i < parameterCount; i++) {
                if (parameters[i].getColumnType().getType() != types[i]) return false;
            }
        }

        cmdPrologue();

        ServerPrepareResult tmpServerPrepareResult = serverPrepareResult;
        try {
            SQLException exception = null;

            //**************************************************************************************
            // send PREPARE if needed
            //**************************************************************************************
            if (serverPrepareResult == null) {
                tmpServerPrepareResult = prepare(sql, true);
            }

            //**************************************************************************************
            // send BULK
            //**************************************************************************************
            int statementId = tmpServerPrepareResult != null ? tmpServerPrepareResult.getStatementId() : -1;

            byte[] lastCmdData = null;
            int index = 0;
            ParameterHolder[] firstParameters = parametersList.get(0);

            do {
                writer.startPacket(0);
                writer.write(COM_STMT_BULK_EXECUTE);
                writer.writeInt(statementId);
                writer.writeShort((byte) 0x80); //always SEND_TYPES_TO_SERVER

                for (ParameterHolder param : firstParameters) {
                    writer.writeShort(param.getColumnType().getType());
                }

                if (lastCmdData != null) {
                    writer.checkMaxAllowedLength(lastCmdData.length);
                    writer.write(lastCmdData);
                    writer.mark();
                    index++;
                    lastCmdData = null;
                }

                for (; index < parametersList.size(); index++) {
                    ParameterHolder[] parameters = parametersList.get(index);
                    for (int i = 0; i < parameterCount; i++) {
                        ParameterHolder holder = parameters[i];
                        if (holder.isNullData()) {
                            writer.write(1); //NULL
                        } else {
                            writer.write(0); //NONE
                            holder.writeBinary(writer);
                        }
                    }

                    //if buffer > MAX_ALLOWED_PACKET, flush until last mark.
                    if (writer.exceedMaxLength() && writer.isMarked()) {
                        writer.flushBufferStopAtMark();
                    }

                    //if flushed, quit loop
                    if (writer.bufferIsDataAfterMark()) break;

                    writer.checkMaxAllowedLength(0);
                    writer.mark();
                }

                if (writer.bufferIsDataAfterMark()) {
                    //flush has been done
                    lastCmdData = writer.resetMark();
                } else {
                    writer.flush();
                    writer.resetMark();
                }

                try {
                    getResult(results);
                } catch (SQLException sqle) {
                    if ("HY000".equals(sqle.getSQLState()) && sqle.getErrorCode() == 1295) {
                        //query contain commands that cannot be handled by BULK protocol
                        // clear error and special error code, so it won't leak anywhere
                        // and wouldn't be misinterpreted as an additional update count
                        results.getCmdInformation().reset();
                        return false;
                    }
                    if (exception == null) {
                        exception = logQuery.exceptionWithQuery(sql, sqle, explicitClosed);
                        if (!options.continueBatchOnError) throw exception;
                    }
                }

            } while (index < parametersList.size() - 1);

            if (lastCmdData != null) {
                writer.startPacket(0);
                writer.write(COM_STMT_BULK_EXECUTE);
                writer.writeInt(statementId);
                writer.writeShort((byte) 0x80); //always SEND_TYPES_TO_SERVER

                for (ParameterHolder param : firstParameters) {
                    writer.writeShort(param.getColumnType().getType());
                }
                writer.write(lastCmdData);
                writer.flush();
                try {
                    getResult(results);
                } catch (SQLException sqle) {
                    if ("HY000".equals(sqle.getSQLState()) && sqle.getErrorCode() == 1295) {
                        //query contain SELECT. cannot be handle by BULK protocol
                        return false;
                    }
                    if (exception == null) {
                        exception = logQuery.exceptionWithQuery(sql, sqle, explicitClosed);
                        if (!options.continueBatchOnError) throw exception;
                    }
                }
            }

            if (exception != null) throw exception;
            results.setRewritten(true);
            return true;

        } catch (IOException e) {
            throw handleIoException(e);
        } finally {
            if (serverPrepareResult == null && tmpServerPrepareResult != null) releasePrepareStatement(tmpServerPrepareResult);
            writer.resetMark();
        }
    }

    private void initializeBatchReader() {
        if (options.useBatchMultiSend && readScheduler == null) {
            synchronized (AbstractQueryProtocol.class) {
                if (readScheduler == null) {
                    readScheduler = SchedulerServiceProviderHolder.getBulkScheduler();
                }
            }
        }
    }

    /**
     * Execute clientPrepareQuery batch.
     *
     * @param results             results
     * @param clientPrepareResult ClientPrepareResult
     * @param parametersList      List of parameters
     * @throws SQLException exception
     */
    private void executeBatchMulti(Results results, final ClientPrepareResult clientPrepareResult,
                                  final List<ParameterHolder[]> parametersList) throws SQLException {

        cmdPrologue();
        initializeBatchReader();
        new AbstractMultiSend(this, writer, results, clientPrepareResult, parametersList) {

            @Override
            public void sendCmd(PacketOutputStream writer, Results results,
                                List<ParameterHolder[]> parametersList, List<String> queries, int paramCount, BulkStatus status,
                                PrepareResult prepareResult)
                    throws SQLException, IOException {

                ParameterHolder[] parameters = parametersList.get(status.sendCmdCounter);
                writer.startPacket(0);
                ComQuery.sendSubCmd(writer, clientPrepareResult, parameters);
                writer.flush();
            }


            @Override
            public SQLException handleResultException(SQLException qex, Results results,
                                                      List<ParameterHolder[]> parametersList, List<String> queries, int currentCounter,
                                                      int sendCmdCounter, int paramCount, PrepareResult prepareResult) {

                int counter = results.getCurrentStatNumber() - 1;
                ParameterHolder[] parameters = parametersList.get(counter);
                List<byte[]> queryParts = clientPrepareResult.getQueryParts();
                StringBuilder sql = new StringBuilder(new String(queryParts.get(0)));

                for (int i = 0; i < paramCount; i++) {
                    sql.append(parameters[i].toString()).append(new String(queryParts.get(i + 1)));
                }

                return logQuery.exceptionWithQuery(sql.toString(), qex, explicitClosed);
            }


            @Override
            public int getParamCount() {
                return clientPrepareResult.getQueryParts().size() - 1;
            }


            @Override
            public int getTotalExecutionNumber() {
                return parametersList.size();
            }

        }.executeBatch();

    }

    /**
     * Execute batch from Statement.executeBatch().
     *
     * @param mustExecuteOnMaster   was intended to be launched on master connection
     * @param results               results
     * @param queries               queries
     * @throws SQLException         if any exception occur
     */
    public void executeBatchStmt(boolean mustExecuteOnMaster, Results results, final List<String> queries)
            throws SQLException {
        cmdPrologue();
        if (this.options.rewriteBatchedStatements) {

            //check that queries are rewritable
            boolean canAggregateSemiColumn = true;
            for (String query : queries) {
                if (!ClientPrepareResult.canAggregateSemiColon(query, noBackslashEscapes())) {
                    canAggregateSemiColumn = false;
                    break;
                }
            }

            if (isInterrupted()) {
                //interrupted by timeout, must throw an exception manually
                throw new SQLTimeoutException("Timeout during batch execution");
            }

            if (canAggregateSemiColumn) {
                executeBatchAggregateSemiColon(results, queries);
            } else {
                executeBatch(results, queries);
            }

        } else {
            executeBatch(results, queries);
        }
    }

    /**
     * Execute list of queries not rewritable.
     *
     * @param results             result object
     * @param queries             list of queries
     * @throws SQLException exception
     */
    private void executeBatch(Results results, final List<String> queries)
            throws SQLException {

        if (!options.useBatchMultiSend) {

            String sql = null;
            SQLException exception = null;

            for (int i = 0; i < queries.size() && !isInterrupted(); i++) {

                try {

                    sql = queries.get(i);
                    writer.startPacket(0);
                    writer.write(COM_QUERY);
                    writer.write(sql);
                    writer.flush();
                    getResult(results);

                } catch (SQLException sqlException) {
                    if (exception == null) {
                        exception = logQuery.exceptionWithQuery(sql, sqlException, explicitClosed);
                        if (!options.continueBatchOnError) throw exception;
                    }
                } catch (IOException e) {
                    if (exception == null) {
                        exception = handleIoException(e);
                        if (!options.continueBatchOnError) throw exception;
                    }
                }
            }
            stopIfInterrupted();

            if (exception != null) throw exception;
            return;
        }
        initializeBatchReader();
        new AbstractMultiSend(this, writer, results, queries) {

            @Override
            public void sendCmd(PacketOutputStream pos, Results results,
                                List<ParameterHolder[]> parametersList, List<String> queries, int paramCount, BulkStatus status,
                                PrepareResult prepareResult)
                    throws SQLException, IOException {

                String sql = queries.get(status.sendCmdCounter);
                pos.startPacket(0);
                pos.write(COM_QUERY);
                pos.write(sql);
                pos.flush();
            }

            @Override
            public SQLException handleResultException(SQLException qex, Results results,
                                                      List<ParameterHolder[]> parametersList, List<String> queries, int currentCounter,
                                                      int sendCmdCounter, int paramCount, PrepareResult prepareResult) {

                String sql = queries.get(currentCounter + sendCmdCounter);
                return logQuery.exceptionWithQuery(sql, qex, explicitClosed);

            }

            @Override
            public int getParamCount() {
                return -1;
            }

            @Override
            public int getTotalExecutionNumber() {
                return queries.size();
            }

        }.executeBatch();

    }

    /**
     * Prepare query on server side.
     * Will permit to know the parameter number of the query, and permit to send only the data on next results.
     * <p>
     * For failover, two additional information are in the result-set object :
     * - current connection : Since server maintain a state of this prepare statement, all query will be executed on this particular connection.
     * - executeOnMaster : state of current connection when creating this prepareStatement (if was on master, will only be executed on master.
     * If was on a slave, can be execute temporary on master, but we keep this flag,
     * so when a slave is connected back to relaunch this query on slave)
     *
     * @param sql             the query
     * @param executeOnMaster state of current connection when creating this prepareStatement
     * @return a ServerPrepareResult object that contain prepare result information.
     * @throws SQLException if any error occur on connection.
     */
    @Override
    public ServerPrepareResult prepare(String sql, boolean executeOnMaster) throws SQLException {

        cmdPrologue();
        lock.lock();
        try {
            if (options.cachePrepStmts) {

                ServerPrepareResult pr = serverPrepareStatementCache.get(database + "-" + sql);

                if (pr != null && pr.incrementShareCounter()) {
                    return pr;
                }

            }
            writer.startPacket(0);
            writer.write(COM_STMT_PREPARE);
            writer.write(sql);
            writer.flush();

            ComStmtPrepare comStmtPrepare = new ComStmtPrepare(this, sql);
            return comStmtPrepare.read(reader, eofDeprecated);
        } catch (IOException e) {
            throw handleIoException(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Execute list of queries.
     * This method is used when using text batch statement and using rewriting (allowMultiQueries || rewriteBatchedStatements).
     * queries will be send to server according to max_allowed_packet size.
     *
     * @param results             result object
     * @param queries             list of queries
     * @throws SQLException exception
     */
    private void executeBatchAggregateSemiColon(Results results, final List<String> queries)
            throws SQLException {

        String firstSql = null;
        int currentIndex = 0;
        int totalQueries = queries.size();
        SQLException exception = null;

        do {

            try {

                firstSql = queries.get(currentIndex++);

                if (totalQueries == 1) {
                    writer.startPacket(0);
                    writer.write(COM_QUERY);
                    writer.write(firstSql);
                    writer.flush();
                } else {
                    currentIndex = ComQuery.sendBatchAggregateSemiColon(writer, firstSql, queries, currentIndex);
                }
                getResult(results);

            } catch (SQLException sqlException) {
                if (exception == null) {
                    exception = logQuery.exceptionWithQuery(firstSql, sqlException, explicitClosed);
                    if (!options.continueBatchOnError) throw exception;
                }
            } catch (IOException e) {
                throw handleIoException(e);
            }
            stopIfInterrupted();

        } while (currentIndex < totalQueries);

        if (exception != null) throw exception;
    }

    /**
     * Specific execution for batch rewrite that has specific query for memory.
     *
     * @param results             result
     * @param prepareResult       prepareResult
     * @param parameterList       parameters
     * @param rewriteValues       is rewritable flag
     * @throws SQLException exception
     */
    private void executeBatchRewrite(Results results,
                                    final ClientPrepareResult prepareResult, List<ParameterHolder[]> parameterList,
                                    boolean rewriteValues) throws SQLException {

        cmdPrologue();

        ParameterHolder[] parameters;
        int currentIndex = 0;
        int totalParameterList = parameterList.size();

        try {

            do {

                currentIndex = ComQuery.sendRewriteCmd(writer, prepareResult.getQueryParts(), currentIndex,
                        prepareResult.getParamCount(), parameterList, rewriteValues);
                getResult(results);

                if (Thread.currentThread().isInterrupted()) {
                    throw new SQLException("Interrupted during batch", INTERRUPTED_EXCEPTION.getSqlState(), -1);
                }

            } while (currentIndex < totalParameterList);

        } catch (SQLException sqlEx) {
            throw logQuery.exceptionWithQuery(sqlEx, prepareResult);
        } catch (IOException e) {
            throw handleIoException(e);
        } finally {
            results.setRewritten(rewriteValues);
        }
    }

    /**
     * Execute Prepare if needed, and execute COM_STMT_EXECUTE queries in batch.
     *
     * @param mustExecuteOnMaster   must normally be executed on master connection
     * @param serverPrepareResult   prepare result. can be null if not prepared.
     * @param results               execution results
     * @param sql                   sql query if needed to be prepared
     * @param parametersList        parameter list
     * @param hasLongData           has long data (stream)
     * @return executed
     * @throws SQLException if parameter error or connection error occur.
     */
    public boolean executeBatchServer(boolean mustExecuteOnMaster, ServerPrepareResult serverPrepareResult,
                                      Results results, String sql, final List<ParameterHolder[]> parametersList,
                                      boolean hasLongData) throws SQLException {

        cmdPrologue();

        if (options.useBulkStmts
                && !hasLongData
                && results.getAutoGeneratedKeys() == Statement.NO_GENERATED_KEYS
                && versionGreaterOrEqual(10,2,7)
                && executeBulkBatch(results, sql, serverPrepareResult, parametersList)) {
            return true;
        }

        if (!options.useBatchMultiSend) return false;
        initializeBatchReader();
        new AbstractMultiSend(this, writer, results, serverPrepareResult, parametersList, true, sql) {
            @Override
            public void sendCmd(PacketOutputStream writer, Results results,
                                List<ParameterHolder[]> parametersList, List<String> queries, int paramCount, BulkStatus status,
                                PrepareResult prepareResult)
                    throws SQLException, IOException {

                ParameterHolder[] parameters = parametersList.get(status.sendCmdCounter);

                //validate parameter set
                if (parameters.length < paramCount) {
                    throw new SQLException("Parameter at position " + (paramCount - 1) + " is not set", "07004");
                }

                //send binary data in a separate stream
                for (int i = 0; i < paramCount; i++) {
                    if (parameters[i].isLongData()) {
                        writer.startPacket(0);
                        writer.write(COM_STMT_SEND_LONG_DATA);
                        writer.writeInt(statementId);
                        writer.writeShort((short) i);
                        parameters[i].writeBinary(writer);
                        writer.flush();
                    }
                }

                writer.startPacket(0);
                ComStmtExecute.writeCmd(statementId, parameters, paramCount, parameterTypeHeader, writer, CURSOR_TYPE_NO_CURSOR);
                writer.flush();
            }

            @Override
            public SQLException handleResultException(SQLException qex, Results results,
                                                      List<ParameterHolder[]> parametersList, List<String> queries, int currentCounter,
                                                      int sendCmdCounter, int paramCount, PrepareResult prepareResult) {
                return logQuery.exceptionWithQuery(qex, prepareResult);
            }

            @Override
            public int getParamCount() {
                return getPrepareResult() == null ? parametersList.get(0).length : ((ServerPrepareResult) getPrepareResult()).getParameters().length;
            }

            @Override
            public int getTotalExecutionNumber() {
                return parametersList.size();
            }

        }.executeBatch();
        return true;
    }

    /**
     * Execute a query that is already prepared.
     *
     * @param mustExecuteOnMaster must execute on master
     * @param serverPrepareResult prepare result
     * @param results             execution result
     * @param parameters          parameters
     * @throws SQLException exception
     */
    @Override
    public void executePreparedQuery(boolean mustExecuteOnMaster, ServerPrepareResult serverPrepareResult, Results results,
                                     ParameterHolder[] parameters)
            throws SQLException {

        cmdPrologue();

        try {

            int parameterCount = serverPrepareResult.getParameters().length;

            //send binary data in a separate stream
            for (int i = 0; i < parameterCount; i++) {
                if (parameters[i].isLongData()) {
                    writer.startPacket(0);
                    writer.write(COM_STMT_SEND_LONG_DATA);
                    writer.writeInt(serverPrepareResult.getStatementId());
                    writer.writeShort((short) i);
                    parameters[i].writeBinary(writer);
                    writer.flush();
                }
            }

            //send execute query
            new ComStmtExecute(serverPrepareResult.getStatementId(), parameters,
                    parameterCount, serverPrepareResult.getParameterTypeHeader(), CURSOR_TYPE_NO_CURSOR)
                    .send(writer);
            getResult(results);

        } catch (SQLException qex) {
            throw logQuery.exceptionWithQuery(parameters, qex, serverPrepareResult);
        } catch (IOException e) {
            throw handleIoException(e);
        }
    }

    /**
     * Rollback transaction.
     */
    public void rollback() throws SQLException {

        cmdPrologue();

        lock.lock();
        try {

            if (inTransaction()) executeQuery("ROLLBACK");

        } catch (Exception e) {
            /* eat exception */
        } finally {
            lock.unlock();
        }
    }

    /**
     * Force release of prepare statement that are not used.
     * This method will be call when adding a new prepare statement in cache, so the packet can be send to server without
     * problem.
     *
     * @param statementId prepared statement Id to remove.
     * @return true if successfully released
     * @throws SQLException if connection exception.
     */
    public boolean forceReleasePrepareStatement(int statementId) throws SQLException {

        if (lock.tryLock()) {

            try {

                checkClose();

                try {
                    writer.startPacket(0);
                    writer.write(COM_STMT_CLOSE);
                    writer.writeInt(statementId);
                    writer.flush();
                    return true;
                } catch (IOException e) {
                    throw new SQLException("Could not deallocate query: " + e.getMessage(), CONNECTION_EXCEPTION.getSqlState(), e);
                }

            } finally {
                lock.unlock();
            }

        } else {
            //lock is used by another thread (bulk reading)
            statementIdToRelease = statementId;
        }

        return false;
    }

    /**
     * Force release of prepare statement that are not used.
     * This permit to deallocate a statement that cannot be release due to multi-thread use.
     *
     * @throws SQLException if connection occur
     */
    public void forceReleaseWaitingPrepareStatement() throws SQLException {
        if (statementIdToRelease != -1 && forceReleasePrepareStatement(statementIdToRelease)) {
            statementIdToRelease = -1;
        }
    }

    @Override
    public boolean ping() throws SQLException {

        cmdPrologue();
        lock.lock();
        try {

            writer.startPacket(0);
            writer.write(COM_PING);
            writer.flush();

            Buffer buffer = reader.getPacket(true);
            return buffer.getByteAt(0) == OK;

        } catch (IOException e) {
            throw new SQLException("Could not ping: " + e.getMessage(), CONNECTION_EXCEPTION.getSqlState(), e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check that connection is valid.
     * !! careful, timeout is in milliseconds, connection.isValid(timeout) is in seconds !!
     *
     * @param timeout timeout in milliseconds
     * @return true is valid
     * @throws SQLException if any error occur
     */
    @Override
    public boolean isValid(int timeout) throws SQLException {

        int initialTimeout = -1;
        try {
            initialTimeout = socket.getSoTimeout();
            socket.setSoTimeout(timeout);
            if (isMasterConnection() && urlParser.isMultiMaster()) {
                //this is a galera node.
                //checking not only that node is responding, but that this node is in primary mode too.
                Results results = new Results();
                executeQuery(true, results, "SELECT @@wsrep_cluster_status");
                results.commandEnd();
                ResultSet rs = results.getResultSet();

                //return true if connected to a galera node that is primary
                return (rs != null && (!rs.next() || "PRIMARY".equalsIgnoreCase(rs.getString(1))));
            }

            return ping();

        } catch (SocketException socketException) {
            throw new SQLException("Could not valid connection : " + socketException.getMessage(),
                    CONNECTION_EXCEPTION.getSqlState(),
                    socketException);
        } finally {

            //set back initial socket timeout
            try {
                if (initialTimeout != -1) socket.setSoTimeout(initialTimeout);
            } catch (SocketException socketException) {
                //eat
            }
        }
    }


    @Override
    public String getCatalog() throws SQLException {

        if ((serverCapabilities & MariaDbServerCapabilities.CLIENT_SESSION_TRACK) != 0) {
            //client session track return empty value, not null value. Java require sending null if empty
            if (database != null && database.isEmpty()) return null;
            return database;
        }

        cmdPrologue();
        lock.lock();
        try {
            Results results = new Results();
            executeQuery(isMasterConnection(), results, "select database()");
            results.commandEnd();
            ResultSet rs = results.getResultSet();
            if (rs.next()) {
                this.database = rs.getString(1);
                return database;
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void setCatalog(final String database) throws SQLException {

        cmdPrologue();

        lock.lock();
        try {

            final SendChangeDbPacket packet = new SendChangeDbPacket(database);
            packet.send(writer);
            final Buffer buffer = reader.getPacket(true);

            if (buffer.getByteAt(0) == ERROR) {
                final ErrorPacket ep = new ErrorPacket(buffer);
                throw new SQLException("Could not select database '" + database + "' : " + ep.getMessage(),
                        ep.getSqlState(), ep.getErrorNumber());
            }

            this.database = database;

        } catch (IOException e) {
            throw handleIoException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void resetDatabase() throws SQLException {
        if (!database.equals(urlParser.getDatabase())) {
            setCatalog(urlParser.getDatabase());
        }
    }

    /**
     * Cancels the current query - clones the current protocol and executes a query using the new connection.
     *
     * @throws SQLException never thrown
     */
    @Override
    public void cancelCurrentQuery() throws SQLException {
        MasterProtocol copiedProtocol = null;
        try {
            copiedProtocol = new MasterProtocol(urlParser, new GlobalStateInfo(), new ReentrantLock());
            copiedProtocol.setHostAddress(getHostAddress());
            copiedProtocol.connect();
            //no lock, because there is already a query running that possessed the lock.
            copiedProtocol.executeQuery("KILL QUERY " + serverThreadId);
        } finally {
            if (copiedProtocol != null) copiedProtocol.close();
        }
        interrupted = true;
    }

    /**
     * Get current autocommit status.
     *
     * @return autocommit status
     */
    @Override
    public boolean getAutocommit() {
        return ((serverStatus & ServerStatus.AUTOCOMMIT) != 0);
    }

    @Override
    public boolean inTransaction() {
        return ((serverStatus & ServerStatus.IN_TRANSACTION) != 0);
    }

    public void closeExplicit() {
        this.explicitClosed = true;
        close();
    }

    /**
     * Deallocate prepare statement if not used anymore.
     *
     * @param serverPrepareResult allocation result
     * @throws SQLException if de-allocation failed.
     */
    @Override
    public void releasePrepareStatement(ServerPrepareResult serverPrepareResult) throws SQLException {
        //If prepared cache is enable, the ServerPrepareResult can be shared in many PrepStatement,
        //so synchronised use count indicator will be decrement.
        serverPrepareResult.decrementShareCounter();

        //deallocate from server if not cached
        if (serverPrepareResult.canBeDeallocate()) {
            forceReleasePrepareStatement(serverPrepareResult.getStatementId());
        }
    }

    /**
     * Set max row return by a statement.
     *
     * @param max row number max value
     */
    public void setInternalMaxRows(long max) {
        if (maxRows != max) maxRows = max;
    }

    public long getMaxRows() {
        return maxRows;
    }

    @Override
    public void setMaxRows(long max) throws SQLException {
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
            this.socket.setSoTimeout(timeout);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Set transaction isolation.
     *
     * @param level transaction level.
     * @throws SQLException if transaction level is unknown
     */
    public void setTransactionIsolation(final int level) throws SQLException {
        cmdPrologue();
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
                    throw new SQLException("Unsupported transaction isolation level");
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

    private void checkClose() throws SQLException {
        if (!this.connected) throw new SQLException("Connection is close", "08000", 1220);
    }

    @Override
    public void getResult(Results results) throws SQLException {

        readPacket(results);

        //load additional results
        while (hasMoreResults()) {
            readPacket(results);
        }

    }

    /**
     * Read server response packet.
     *
     * @param results result object
     * @throws SQLException if sub-result connection fail
     * @see <a href="https://mariadb.com/kb/en/mariadb/4-server-response-packets/">server response packets</a>
     */
    private void readPacket(Results results) throws SQLException {
        Buffer buffer;
        try {
            buffer = reader.getPacket(true);
        } catch (IOException e) {
            throw handleIoException(e);
        }

        switch (buffer.getByteAt(0)) {

            //*********************************************************************************************************
            //* OK response
            //*********************************************************************************************************
            case OK:
                readOkPacket(buffer, results);
                break;

            //*********************************************************************************************************
            //* ERROR response
            //*********************************************************************************************************
            case ERROR:
                throw readErrorPacket(buffer, results);

                //*********************************************************************************************************
                //* LOCAL INFILE response
                //*********************************************************************************************************
            case LOCAL_INFILE:
                readLocalInfilePacket(buffer, results);
                break;

            //*********************************************************************************************************
            //* ResultSet
            //*********************************************************************************************************
            default:
                readResultSet(buffer, results);
                break;

        }

    }

    /**
     * Read OK_Packet.
     *
     * @param buffer  current buffer
     * @param results result object
     * @see <a href="https://mariadb.com/kb/en/mariadb/ok_packet/">OK_Packet</a>
     */
    private void readOkPacket(Buffer buffer, Results results) {
        buffer.skipByte(); //fieldCount
        final long updateCount = buffer.getLengthEncodedNumeric();
        final long insertId = buffer.getLengthEncodedNumeric();

        serverStatus = buffer.readShort();
        hasWarnings = (buffer.readShort() > 0);

        if ((serverStatus & ServerStatus.SERVER_SESSION_STATE_CHANGED) != 0) {
            handleStateChange(buffer, results);
        }

        results.addStats(updateCount, insertId, hasMoreResults());
    }

    private void handleStateChange(Buffer buf, Results results) {
        buf.skipLengthEncodedBytes(); //info
        while (buf.remaining() > 0) {
            Buffer stateInfo = buf.getLengthEncodedBuffer();
            if (stateInfo.remaining() > 0) {
                switch (stateInfo.readByte()) {

                    case StateChange.SESSION_TRACK_SYSTEM_VARIABLES:
                        Buffer sessionVariableBuf = stateInfo.getLengthEncodedBuffer();
                        String variable = sessionVariableBuf.readStringLengthEncoded(Buffer.UTF_8);
                        String value = sessionVariableBuf.readStringLengthEncoded(Buffer.UTF_8);
                        logger.debug("System variable change :  {} = {}", variable, value);

                        //only variable uses
                        if ("auto_increment_increment".equals(variable)) {
                            autoIncrementIncrement = Integer.parseInt(value);
                            results.setAutoIncrement(autoIncrementIncrement);
                        }
                        break;

                    case StateChange.SESSION_TRACK_SCHEMA:
                        Buffer sessionSchemaBuf = stateInfo.getLengthEncodedBuffer();
                        database = sessionSchemaBuf.readStringLengthEncoded(Buffer.UTF_8);
                        logger.debug("Database change : now is '{}'", database);
                        break;

                    default:
                        stateInfo.skipLengthEncodedBytes();
                }
            }
        }

    }

    /**
     * Get current auto increment increment.
     * *** no lock needed ****
     *
     * @return auto increment increment.
     * @throws SQLException if cannot retrieve auto increment value
     */
    public int getAutoIncrementIncrement() throws SQLException {
        if (autoIncrementIncrement == 0) {
            lock.lock();
            try {
                Results results = new Results();
                executeQuery(true, results, "select @@auto_increment_increment");
                results.commandEnd();
                ResultSet rs = results.getResultSet();
                rs.next();
                autoIncrementIncrement = rs.getInt(1);
            } catch (SQLException e) {
                if (e.getSQLState().startsWith("08")) throw e;
                autoIncrementIncrement = 1;
            } finally {
                lock.unlock();
            }
        }
        return autoIncrementIncrement;
    }


    /**
     * Read ERR_Packet.
     *
     * @param buffer  current buffer
     * @param results result object
     * @return SQLException if sub-result connection fail
     * @see <a href="https://mariadb.com/kb/en/mariadb/err_packet/">ERR_Packet</a>
     */
    private SQLException readErrorPacket(Buffer buffer, Results results) {
        removeHasMoreResults();
        this.hasWarnings = false;
        buffer.skipByte();
        int errorNumber = buffer.readShort();
        String message;
        String sqlState;
        if (buffer.readByte() == '#') {
            sqlState = new String(buffer.readRawBytes(5));
            message = buffer.readStringNullEnd(Buffer.UTF_8);
        } else {
            // Pre-4.1 message, still can be output in newer versions (e.g with 'Too many connections')
            buffer.position -= 1;
            message = new String(buffer.buf, buffer.position, buffer.limit - buffer.position, Buffer.UTF_8);
            sqlState = "HY000";
        }
        results.addStatsError(false);
        removeActiveStreamingResult();
        return new SQLException(message, sqlState, errorNumber);
    }

    /**
     * Read Local_infile Packet.
     *
     * @param buffer  current buffer
     * @param results result object
     * @throws SQLException if sub-result connection fail
     * @see <a href="https://mariadb.com/kb/en/mariadb/local_infile-packet/">local_infile packet</a>
     */
    private void readLocalInfilePacket(Buffer buffer, Results results) throws SQLException {

        int seq = 2;
        buffer.getLengthEncodedNumeric(); //field pos
        String fileName = buffer.readStringNullEnd(Buffer.UTF_8);
        try {
            // Server request the local file (LOCAL DATA LOCAL INFILE)
            // We do accept general URLs, too. If the localInfileStream is
            // set, use that.
            InputStream is;
            writer.startPacket(seq);
            if (localInfileInputStream == null) {

                if (!getUrlParser().getOptions().allowLocalInfile) {
                    writer.writeEmptyPacket();
                    reader.getPacket(true);
                    throw new SQLException(
                            "Usage of LOCAL INFILE is disabled. To use it enable it via the connection property allowLocalInfile=true",
                            FEATURE_NOT_SUPPORTED.getSqlState(), -1);
                }

                //validate all defined interceptors
                ServiceLoader<LocalInfileInterceptor> loader = ServiceLoader.load(LocalInfileInterceptor.class);
                for (LocalInfileInterceptor interceptor : loader) {
                    if (!interceptor.validate(fileName)) {
                        writer.writeEmptyPacket();
                        reader.getPacket(true);
                        throw new SQLException("LOCAL DATA LOCAL INFILE request to send local file named \""
                                + fileName + "\" not validated by interceptor \"" + interceptor.getClass().getName()
                                + "\"");
                    }
                }

                try {
                    URL url = new URL(fileName);
                    is = url.openStream();
                } catch (IOException ioe) {
                    try {
                        is = new FileInputStream(fileName);
                    } catch (FileNotFoundException f) {
                        writer.writeEmptyPacket();
                        reader.getPacket(true);
                        throw new SQLException("Could not send file : " + f.getMessage(), "22000", -1, f);
                    }
                }
            } else {
                is = localInfileInputStream;
                localInfileInputStream = null;
            }

            try {

                byte[] buf = new byte[8192];
                int len;
                while ((len = is.read(buf)) > 0) {
                    writer.startPacket(seq++);
                    writer.write(buf, 0, len);
                    writer.flush();
                }
                writer.writeEmptyPacket();

            } catch (IOException ioe) {
                throw handleIoException(ioe);
            } finally {
                is.close();
            }

            getResult(results);

        } catch (IOException e) {
            throw handleIoException(e);
        }
    }

    /**
     * Read ResultSet Packet.
     *
     * @param buffer  current buffer
     * @param results result object
     * @throws SQLException if sub-result connection fail
     * @see <a href="https://mariadb.com/kb/en/mariadb/resultset/">resultSet packets</a>
     */
    private void readResultSet(Buffer buffer, Results results) throws SQLException {
        long fieldCount = buffer.getLengthEncodedNumeric();

        try {

            //read columns information's
            ColumnInformation[] ci = new ColumnInformation[(int) fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                ci[i] = new ColumnInformation(reader.getPacket(false));
            }

            boolean callableResult = false;
            if (!eofDeprecated) {
                //read EOF packet
                //EOF status is mandatory because :
                // - Call query will have an callable resultSet for OUT parameters
                //   -> this resultSet must be identified and not listed in JDBC statement.getResultSet()
                // - after a callable resultSet, a OK packet is send, but mysql does send the  a bad "more result flag"
                Buffer bufferEof = reader.getPacket(true);
                if (bufferEof.readByte() != EOF) {
                    throw new SQLException("Packets out of order when reading field packets, expected was EOF stream."
                            + ((options.enablePacketDebug) ? getTraces() : "Packet contents (hex) = "
                                + Utils.hexdump(options.maxQuerySizeToLog, 0, bufferEof.position, bufferEof.buf)));
                }
                bufferEof.skipBytes(2); //Skip warningCount
                callableResult = (bufferEof.readShort() & ServerStatus.PS_OUT_PARAMETERS) != 0;
            }

            //read resultSet
            SelectResultSet selectResultSet;
            if (results.getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY) {
                selectResultSet = new SelectResultSet(ci, results, this, reader, callableResult, eofDeprecated);
            } else {
                //remove fetch size to permit updating results without creating new connection
                results.removeFetchSize();
                selectResultSet = new UpdatableResultSet(ci, results, this, reader, callableResult, eofDeprecated);
            }

            results.addResultSet(selectResultSet, hasMoreResults() || results.getFetchSize() > 0);

        } catch (IOException e) {
            throw handleIoException(e);
        }
    }

    public void prologProxy(ServerPrepareResult serverPrepareResult, long maxRows, boolean hasProxy,
                            MariaDbConnection connection, MariaDbStatement statement) throws SQLException {
        prolog(maxRows, hasProxy, connection, statement);
    }

    /**
     * Preparation before command.
     *
     * @param maxRows    query max rows
     * @param hasProxy   has proxy
     * @param connection current connection
     * @param statement  current statement
     * @throws SQLException if any error occur.
     */
    public void prolog(long maxRows, boolean hasProxy, MariaDbConnection connection, MariaDbStatement statement)
            throws SQLException {
        if (explicitClosed) {
            throw new SQLException("execute() is called on closed connection");
        }
        //old failover handling
        if (!hasProxy && shouldReconnectWithoutProxy()) {
            try {
                connectWithoutProxy();
            } catch (SQLException qe) {
                ExceptionMapper.throwException(qe, connection, statement);
            }
        }

        try {
            setMaxRows(maxRows);
        } catch (SQLException qe) {
            ExceptionMapper.throwException(qe, connection, statement);
        }

        connection.reenableWarnings();
    }

    public ServerPrepareResult addPrepareInCache(String key, ServerPrepareResult serverPrepareResult) {
        return serverPrepareStatementCache.put(key, serverPrepareResult);
    }


    private void cmdPrologue() throws SQLException {

        //load active result if any so buffer are clean for next query
        if (activeStreamingResult != null) {
            activeStreamingResult.loadFully(false, this);
            activeStreamingResult = null;
        }

        if (activeFutureTask != null) {
            //wait for remaining batch result to be read, to ensure correct connection state
            try {
                activeFutureTask.get();
            } catch (ExecutionException executionException) {
                //last batch exception are to be discarded
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted reading remaining batch response ",
                        INTERRUPTED_EXCEPTION.getSqlState(), -1, interruptedException);
            } finally {
                //bulk can prepare, and so if prepare cache is enable, can replace an already cached prepareStatement
                //this permit to release those old prepared statement without conflict.
                forceReleaseWaitingPrepareStatement();
            }
            activeFutureTask = null;
        }

        if (!this.connected) throw new SQLException("Connection is closed", "08000", 1220);
        interrupted = false;

    }

    /**
     * Set current state after a failover.
     *
     * @param maxRows                   current Max rows
     * @param transactionIsolationLevel current transactionIsolationLevel
     * @param database                  current database
     * @param autocommit                current autocommit state
     * @throws SQLException if any error occur.
     */
    //TODO set all client affected variables when implementing CONJ-319
    public void resetStateAfterFailover(long maxRows, int transactionIsolationLevel, String database, boolean autocommit) throws SQLException {
        setMaxRows(maxRows);

        if (transactionIsolationLevel != 0) {
            setTransactionIsolation(transactionIsolationLevel);
        }

        if (database != null && !"".equals(database) && !getDatabase().equals(database)) {
            setCatalog(database);
        }

        if (getAutocommit() != autocommit) {
            executeQuery("set autocommit=" + (autocommit ? "1" : "0"));
        }
    }

    /**
     * Handle IoException (reconnect if Exception is due to having send too much data,
     * making server close the connection.
     * <p>
     * There is 3 kind of IOException :
     * <ol>
     * <li> MaxAllowedPacketException :
     * without need of reconnect : thrown when driver don't send packet that would have been too big
     * then error is not a CONNECTION_EXCEPTION</li>
     * <li>packets size is greater than max_allowed_packet (can be checked with writer.isAllowedCmdLength()). Need to reconnect</li>
     * <li>unknown IO error throw a CONNECTION_EXCEPTION</li>
     * </ol>
     *
     * @param initialException initial Io error
     * @return the resulting error to return to client.
     */
    public SQLException handleIoException(IOException initialException) {
        boolean mustReconnect;
        boolean driverPreventError = false;

        if (MaxAllowedPacketException.class.isInstance(initialException)) {
            mustReconnect = ((MaxAllowedPacketException) initialException).isMustReconnect();
            driverPreventError = !mustReconnect;
        } else {
            mustReconnect = writer.exceedMaxLength();
        }

        if (mustReconnect) {
            try {
                connect();
            } catch (SQLException queryException) {
                return new SQLNonTransientConnectionException(initialException.getMessage()
                        + "\nError during reconnection" + getTraces(), CONNECTION_EXCEPTION.getSqlState(), initialException);
            }

            try {
                resetStateAfterFailover(getMaxRows(), getTransactionIsolationLevel(), getDatabase(), getAutocommit());
            } catch (SQLException queryException) {
                return new SQLException("reconnection succeed, but resetting previous state failed",
                        UNDEFINED_SQLSTATE.getSqlState() + getTraces(), initialException);
            }

            return new SQLException("Could not send query: query size is >= to max_allowed_packet ("
                    + writer.getMaxAllowedPacket() + ")" + getTraces(), UNDEFINED_SQLSTATE.getSqlState(), initialException);
        }

        return new SQLException(initialException.getMessage() + getTraces(),
                driverPreventError ? UNDEFINED_SQLSTATE.getSqlState() : CONNECTION_EXCEPTION.getSqlState(), initialException);

    }

    public void setActiveFutureTask(FutureTask activeFutureTask) {
        this.activeFutureTask = activeFutureTask;
    }

    public boolean isInterrupted() {
        return interrupted;
    }

    /**
     * Throw TimeoutException if timeout has been reached.
     *
     * @throws SQLTimeoutException to indicate timeout exception.
     */
    public void stopIfInterrupted() throws SQLTimeoutException {
        if (isInterrupted()) {
            //interrupted during read, must throw an exception manually
            throw new SQLTimeoutException("Timeout during batch execution");
        }
    }
}
