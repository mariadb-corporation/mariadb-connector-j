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

package org.mariadb.jdbc.internal.failover;

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.failover.tools.SearchFilter;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Set;

public interface Listener {
    FailoverProxy getProxy();

    void setProxy(FailoverProxy proxy);

    void initializeConnection() throws QueryException;

    void preExecute() throws QueryException;

    void preClose() throws SQLException;

    void reconnectFailedConnection(SearchFilter filter) throws QueryException;

    void switchReadOnlyConnection(Boolean readonly) throws QueryException;

    HandleErrorResult primaryFail(Method method, Object[] args) throws Throwable;

    Object invoke(Method method, Object[] args, Protocol specificProtocol) throws Throwable;

    Object invoke(Method method, Object[] args) throws Throwable;

    HandleErrorResult handleFailover(QueryException qe, Method method, Object[] args, Protocol protocol) throws Throwable;

    void foundActiveMaster(Protocol protocol) throws QueryException;

    Set<HostAddress> getBlacklistKeys();

    void addToBlacklist(HostAddress hostAddress);

    void removeFromBlacklist(HostAddress hostAddress);

    void syncConnection(Protocol from, Protocol to) throws QueryException;

    UrlParser getUrlParser();

    void throwFailoverMessage(HostAddress failHostAddress, boolean wasMaster, QueryException queryException,
                              boolean reconnected) throws QueryException;

    boolean isAutoReconnect();

    int getRetriesAllDown();

    boolean isExplicitClosed();

    void reconnect() throws QueryException;

    boolean isReadOnly();

    boolean isClosed();

    Protocol getCurrentProtocol();

    boolean hasHostFail();

    boolean canRetryFailLoop();

    SearchFilter getFilterForFailedHost();

    boolean isMasterConnected();

    boolean setMasterHostFail();

    boolean isMasterHostFail();

    long getLastQueryNanos();

    boolean checkMasterStatus(SearchFilter searchFilter);

    void rePrepareOnSlave(ServerPrepareResult oldServerPrepareResult, boolean mustExecuteOnMaster) throws QueryException;
}
