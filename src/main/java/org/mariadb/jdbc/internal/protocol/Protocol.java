package org.mariadb.jdbc.internal.protocol;

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

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.failover.FailoverProxy;
import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.util.PrepareStatementCache;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.queryresults.AbstractQueryResult;
import org.mariadb.jdbc.internal.query.Query;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.util.dao.PrepareResult;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.Calendar;
import java.util.List;

public interface Protocol {
    PrepareResult prepare(String sql) throws QueryException;

    void closePreparedStatement(int statementId) throws QueryException;

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

    void rollback();

    String getDatabase();

    String getUsername();

    String getPassword();

    boolean ping() throws QueryException;

    AbstractQueryResult executeQuery(Query query) throws QueryException;

    AbstractQueryResult executeQuery(final List<Query> queries, boolean streaming, boolean isRewritable, int rewriteOffset) throws QueryException;

    AbstractQueryResult executeQuery(Query query, boolean streaming) throws QueryException;

    AbstractQueryResult getResult(Object queryObj, boolean streaming, boolean binaryProtocol) throws QueryException;

    void cancelCurrentQuery() throws QueryException, IOException;

    AbstractQueryResult getMoreResults(boolean streaming) throws QueryException;

    boolean hasUnreadData();

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

    void closeIfActiveResult();

    void connectWithoutProxy() throws QueryException;

    boolean shouldReconnectWithoutProxy();

    void setHostFailedWithoutProxy();

    AbstractQueryResult executePreparedQuery(String sql, ParameterHolder[] parameters, PrepareResult prepareResult, MariaDbType[] parameterTypeHeader,
                                             boolean isStreaming) throws QueryException;

    void releasePrepareStatement(String sql, int statementId) throws QueryException;

    AbstractQueryResult executePreparedQueryAfterFailover(String sql, ParameterHolder[] parameters, PrepareResult oldPrepareResult,
                                                          MariaDbType[] parameterTypeHeader, boolean isStreaming) throws QueryException; //used

    PrepareStatementCache prepareStatementCache();


    String getServerData(String code);

    Calendar getCalendar();

}
