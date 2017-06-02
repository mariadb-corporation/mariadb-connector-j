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

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.MariaDbStatement;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.com.read.dao.Results;
import org.mariadb.jdbc.internal.com.send.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.failover.FailoverProxy;
import org.mariadb.jdbc.internal.io.input.PacketInputStream;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;
import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.util.ServerPrepareStatementCache;
import org.mariadb.jdbc.internal.util.dao.ClientPrepareResult;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.ReentrantLock;

public interface Protocol {
    ServerPrepareResult prepare(String sql, boolean executeOnMaster) throws SQLException;

    boolean getAutocommit();

    boolean noBackslashEscapes();

    void connect() throws SQLException;

    UrlParser getUrlParser();

    boolean inTransaction();

    FailoverProxy getProxy();

    void setProxy(FailoverProxy proxy);

    Options getOptions();

    boolean hasMoreResults();

    void close();

    void closeExplicit();

    boolean isClosed();

    void setCatalog(String database) throws SQLException;

    String getServerVersion();

    boolean isConnected();

    boolean getReadonly();

    void setReadonly(boolean readOnly) throws SQLException;

    boolean isMasterConnection();

    boolean mustBeMasterConnection();

    HostAddress getHostAddress();

    void setHostAddress(HostAddress hostAddress);

    String getHost();

    int getPort();

    void rollback() throws SQLException;

    String getDatabase();

    String getUsername();

    String getPassword();

    boolean ping() throws SQLException;

    void executeQuery(String sql) throws SQLException;

    void executeQuery(boolean mustExecuteOnMaster, Results results, final String sql) throws SQLException;

    void executeQuery(boolean mustExecuteOnMaster, Results results, final String sql, Charset charset) throws SQLException;

    void executeQuery(boolean mustExecuteOnMaster, Results results, final ClientPrepareResult clientPrepareResult,
                      ParameterHolder[] parameters) throws SQLException;

    void executeQuery(boolean mustExecuteOnMaster, Results results, final ClientPrepareResult clientPrepareResult,
                      ParameterHolder[] parameters, int timeout) throws SQLException;

    void executeBatchMulti(boolean mustExecuteOnMaster, Results results, final ClientPrepareResult clientPrepareResult,
                           List<ParameterHolder[]> parameterList) throws SQLException;

    void executeBatch(boolean mustExecuteOnMaster, Results results, List<String> queries) throws SQLException;

    void executeBatchMultiple(boolean mustExecuteOnMaster, Results results, List<String> queries) throws SQLException;

    void executeBatchRewrite(boolean mustExecuteOnMaster, Results results, final ClientPrepareResult prepareResult,
                             List<ParameterHolder[]> parameterList, boolean rewriteValues) throws SQLException;


    void executePreparedQuery(boolean mustExecuteOnMaster, ServerPrepareResult serverPrepareResult,
                              Results results, ParameterHolder[] parameters) throws SQLException;

    ServerPrepareResult prepareAndExecutes(boolean mustExecuteOnMaster, ServerPrepareResult serverPrepareResult,
                                           Results results, String sql,
                                           List<ParameterHolder[]> parameterList) throws SQLException;

    ServerPrepareResult prepareAndExecute(boolean mustExecuteOnMaster, ServerPrepareResult serverPrepareResult,
                                          Results results, String sql, ParameterHolder[] parameters) throws SQLException;

    void getResult(Results results) throws SQLException;

    void cancelCurrentQuery() throws SQLException, IOException;

    void skip() throws SQLException, SQLException;

    boolean checkIfMaster() throws SQLException;

    boolean hasWarnings();

    int getDataTypeMappingFlags();

    void setInternalMaxRows(long max);

    long getMaxRows();

    void setMaxRows(long max) throws SQLException;

    int getMajorServerVersion();

    int getMinorServerVersion();

    boolean versionGreaterOrEqual(int major, int minor, int patch);

    void setLocalInfileInputStream(InputStream inputStream);

    int getTimeout() throws SocketException;

    void setTimeout(int timeout) throws SocketException;

    boolean getPinGlobalTxToPhysicalConnection();

    long getServerThreadId();

    void setTransactionIsolation(int level) throws SQLException;

    int getTransactionIsolationLevel();

    boolean isExplicitClosed();

    void connectWithoutProxy() throws SQLException;

    boolean shouldReconnectWithoutProxy();

    void setHostFailedWithoutProxy();

    void releasePrepareStatement(ServerPrepareResult serverPrepareResult) throws SQLException;

    boolean forceReleasePrepareStatement(int statementId) throws SQLException;

    void forceReleaseWaitingPrepareStatement() throws SQLException;

    ServerPrepareStatementCache prepareStatementCache();

    TimeZone getTimeZone();

    void prolog(long maxRows, boolean hasProxy, MariaDbConnection connection,
                MariaDbStatement statement) throws SQLException;

    void prologProxy(ServerPrepareResult serverPrepareResult, long maxRows, boolean hasProxy,
                     MariaDbConnection connection, MariaDbStatement statement) throws SQLException;

    Results getActiveStreamingResult();

    void setActiveStreamingResult(Results mariaSelectResultSet);

    ReentrantLock getLock();

    void setServerStatus(short serverStatus);

    void removeHasMoreResults();

    void setHasWarnings(boolean hasWarnings);

    ServerPrepareResult addPrepareInCache(String key, ServerPrepareResult serverPrepareResult);

    void readEofPacket() throws SQLException, IOException;

    void skipEofPacket() throws SQLException, IOException;

    void changeSocketTcpNoDelay(boolean setTcpNoDelay) throws SocketException;

    void changeSocketSoTimeout(int setSoTimeout) throws SocketException;

    void removeActiveStreamingResult();

    void resetStateAfterFailover(long maxRows, int transactionIsolationLevel, String database, boolean autocommit)
            throws SQLException;

    void setActiveFutureTask(FutureTask activeFutureTask);

    boolean isServerMariaDb();

    SQLException handleIoException(IOException initialException);

    PacketInputStream getReader();

    PacketOutputStream getWriter();

    boolean isEofDeprecated();

    int getAutoIncrementIncrement();

    boolean sessionStateAware();

    String getTraces();

    boolean isInterrupted();
}