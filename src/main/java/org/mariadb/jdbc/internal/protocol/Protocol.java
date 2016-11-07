package org.mariadb.jdbc.internal.protocol;

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

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.MariaDbStatement;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.failover.FailoverProxy;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.queryresults.ExecutionResult;
import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;
import org.mariadb.jdbc.internal.util.BulkStatus;
import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.util.ServerPrepareStatementCache;
import org.mariadb.jdbc.internal.util.dao.ClientPrepareResult;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public interface Protocol {
    ServerPrepareResult prepare(String sql, boolean executeOnMaster) throws QueryException;

    boolean getAutocommit();

    boolean noBackslashEscapes();

    void connect() throws QueryException;

    UrlParser getUrlParser();

    boolean inTransaction();

    FailoverProxy getProxy();

    void setProxy(FailoverProxy proxy);

    Options getOptions();

    boolean hasMoreResults();

    void close();

    void closeExplicit();

    boolean isClosed();

    void setCatalog(String database) throws QueryException;

    String getServerVersion();

    boolean isConnected();

    boolean getReadonly();

    void setReadonly(boolean readOnly) throws QueryException;

    boolean isMasterConnection();

    boolean mustBeMasterConnection();

    HostAddress getHostAddress();

    void setHostAddress(HostAddress hostAddress);

    String getHost();

    int getPort();

    void rollback() throws QueryException;

    String getDatabase();

    String getUsername();

    String getPassword();

    boolean ping() throws QueryException;

    void executeQuery(String sql) throws QueryException;

    void executeQuery(boolean mustExecuteOnMaster, ExecutionResult executionResult, final String sql, int resultSetScrollType) throws QueryException;

    void executeQuery(boolean mustExecuteOnMaster, ExecutionResult executionResult, final ClientPrepareResult clientPrepareResult,
                      ParameterHolder[] parameters, int resultSetScrollType) throws QueryException;

    void executeBatchMulti(boolean mustExecuteOnMaster, ExecutionResult executionResult, final ClientPrepareResult clientPrepareResult,
                           List<ParameterHolder[]> parameterList, int resultSetScrollType) throws QueryException;

    void executeBatch(boolean mustExecuteOnMaster, ExecutionResult executionResult, List<String> queries, int resultSetScrollType)
            throws QueryException;

    void executeBatchMultiple(boolean mustExecuteOnMaster, ExecutionResult executionResult, List<String> queries,
                              int resultSetScrollType) throws QueryException;

    void executeBatchRewrite(boolean mustExecuteOnMaster, ExecutionResult executionResult, final ClientPrepareResult prepareResult,
                             List<ParameterHolder[]> parameterList,
                             int resultSetScrollType, boolean rewriteValues) throws QueryException;


    void executePreparedQuery(boolean mustExecuteOnMaster, ServerPrepareResult serverPrepareResult,
                              ExecutionResult executionResult, ParameterHolder[] parameters,
                              int resultSetScrollType) throws QueryException;

    ServerPrepareResult prepareAndExecutes(boolean mustExecuteOnMaster, ServerPrepareResult serverPrepareResult,
                                           ExecutionResult executionResult, String sql,
                                           List<ParameterHolder[]> parameterList, int resultSetScrollType) throws QueryException;

    ServerPrepareResult prepareAndExecute(boolean mustExecuteOnMaster, ServerPrepareResult serverPrepareResult,
                                          ExecutionResult executionResult, String sql,
                                          ParameterHolder[] parameters, int resultSetScrollType) throws QueryException;

    ExecutionResult getResult(ExecutionResult executionResult, int resultSetScrollType, boolean binaryProtocol, boolean loadAllResults)
            throws QueryException;

    void cancelCurrentQuery() throws QueryException, IOException;

    void skip() throws SQLException, QueryException;

    boolean checkIfMaster() throws QueryException;

    boolean hasWarnings();

    int getDataTypeMappingFlags();

    void setInternalMaxRows(int max);

    int getMaxRows();

    void setMaxRows(int max) throws QueryException;

    int getMajorServerVersion();

    int getMinorServerVersion();

    boolean versionGreaterOrEqual(int major, int minor, int patch);

    void setLocalInfileInputStream(InputStream inputStream);

    int getTimeout() throws SocketException;

    void setTimeout(int timeout) throws SocketException;

    boolean getPinGlobalTxToPhysicalConnection();

    long getServerThreadId();

    void setTransactionIsolation(int level) throws QueryException;

    int getTransactionIsolationLevel();

    boolean isExplicitClosed();

    void connectWithoutProxy() throws QueryException;

    boolean shouldReconnectWithoutProxy();

    void setHostFailedWithoutProxy();

    void releasePrepareStatement(ServerPrepareResult serverPrepareResult) throws QueryException;

    boolean forceReleasePrepareStatement(int statementId) throws QueryException;

    void forceReleaseWaitingPrepareStatement() throws QueryException;

    ServerPrepareStatementCache prepareStatementCache();


    String getServerData(String code);

    Calendar getCalendar();

    void prolog(ExecutionResult executionResult, int maxRows, boolean hasProxy, MariaDbConnection connection,
                MariaDbStatement statement) throws SQLException;

    void prologProxy(ServerPrepareResult serverPrepareResult, ExecutionResult executionResult, int maxRows, boolean hasProxy,
                     MariaDbConnection connection, MariaDbStatement statement) throws SQLException;

    MariaSelectResultSet getActiveStreamingResult();

    void setActiveStreamingResult(MariaSelectResultSet mariaSelectResultSet);

    ReentrantLock getLock();

    void getMoreResults(ExecutionResult executionResult) throws QueryException;

    void setMoreResults(boolean moreResults, boolean moreResultsTypeBinary);

    void setHasWarnings(boolean hasWarnings);

    void releaseWriterBuffer();

    ByteBuffer getWriter();

    ServerPrepareResult addPrepareInCache(String key, ServerPrepareResult serverPrepareResult);

    void readEofPacket() throws QueryException, IOException;

    void skipEofPacket() throws QueryException, IOException;

    ReadPacketFetcher getPacketFetcher();

    void changeSocketTcpNoDelay(boolean setTcpNoDelay) throws SocketException;

    void changeSocketSoTimeout(int setSoTimeout) throws SocketException;

    ServerPrepareResult getPrepareStatementFromCache(String key);
}