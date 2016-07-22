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

package org.mariadb.jdbc.internal.protocol;

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.failover.FailoverProxy;
import org.mariadb.jdbc.internal.failover.impl.AuroraListener;
import org.mariadb.jdbc.internal.failover.tools.SearchFilter;
import org.mariadb.jdbc.internal.queryresults.SingleExecutionResult;
import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;
import org.mariadb.jdbc.internal.util.ExceptionMapper;
import org.mariadb.jdbc.internal.util.dao.QueryException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class AuroraProtocol extends MastersSlavesProtocol {

    public AuroraProtocol(final UrlParser url, final ReentrantLock lock) {
        super(url, lock);
    }

    /**
     * Connect aurora probable master.
     * Aurora master change in time. The only way to check that a server is a master is to asked him.
     *
     * @param listener aurora failover to call back if master is found
     * @param probableMaster probable master host
     */
    public static void searchProbableMaster(AuroraListener listener, HostAddress probableMaster) {
        AuroraProtocol protocol = getNewProtocol(listener.getProxy(), listener.getUrlParser());
        try {

            protocol.setHostAddress(probableMaster);
            protocol.connect();
            listener.removeFromBlacklist(protocol.getHostAddress());

            if (listener.isMasterHostFailReconnect() && protocol.isMasterConnection()) {
                protocol.setMustBeMasterConnection(true);
                listener.foundActiveMaster(protocol);
            } else if (listener.isSecondaryHostFailReconnect() && !protocol.isMasterConnection()) {
                protocol.setMustBeMasterConnection(false);
                listener.foundActiveSecondary(protocol);
            } else {
                protocol.close();
                protocol = getNewProtocol(listener.getProxy(), listener.getUrlParser());
            }

        } catch (QueryException e) {
            listener.addToBlacklist(protocol.getHostAddress());
        }
    }

    /**
     * loop until found the failed connection.
     *
     * @param listener     current failover
     * @param addresses    list of HostAddress to loop
     * @param searchFilter search parameter
     * @throws QueryException if not found
     */
    public static void loop(AuroraListener listener, final List<HostAddress> addresses, SearchFilter searchFilter)
            throws QueryException {

        AuroraProtocol protocol;
        ArrayDeque<HostAddress> loopAddresses = new ArrayDeque<>((!addresses.isEmpty()) ? addresses : listener.getBlacklistKeys());
        if (loopAddresses.isEmpty()) {
            loopAddresses.addAll(listener.getUrlParser().getHostAddresses());
        }
        int maxConnectionTry = listener.getRetriesAllDown();
        QueryException lastQueryException = null;
        HostAddress probableMasterHost = null;

        // Only one address means cluster so only possible connection
        if (listener.getClusterHostAddress() != null && loopAddresses.size() != 1) {
            loopAddresses.remove(listener.getClusterHostAddress());
            // put cluster at end if only two addresses to use as backup
            if (loopAddresses.size() == 1) {
                loopAddresses.add(listener.getClusterHostAddress());
            }
        }

        while (!loopAddresses.isEmpty() || (!searchFilter.isFailoverLoop() && maxConnectionTry > 0)) {
            protocol = getNewProtocol(listener.getProxy(), listener.getUrlParser());

            if (listener.isExplicitClosed() || (!listener.isSecondaryHostFailReconnect() && !listener.isMasterHostFailReconnect())) {
                return;
            }
            maxConnectionTry--;

            try {
                HostAddress host = loopAddresses.pollFirst();
                if (host == null) {
                    for (HostAddress hostAddress : listener.getUrlParser().getHostAddresses()) {
                        if (!hostAddress.equals(listener.getClusterHostAddress())) {
                            loopAddresses.add(hostAddress);
                        }
                    }
                    // Use cluster last as backup
                    if (listener.getClusterHostAddress() != null && listener.getUrlParser().getHostAddresses().size() <= 2) {
                        loopAddresses.add(listener.getClusterHostAddress());
                    }

                    host = loopAddresses.pollFirst();
                }
                protocol.setHostAddress(host);
                protocol.connect();

                if (listener.isExplicitClosed()) {
                    protocol.close();
                    return;
                }

                listener.removeFromBlacklist(protocol.getHostAddress());

                if (listener.isMasterHostFailReconnect() && protocol.isMasterConnection()) {
                    // Look for secondary when only known endpoint is the cluster endpoint
                    if (searchFilter.isFineIfFoundOnlyMaster() && listener.getUrlParser().getHostAddresses().size() == 1
                            && protocol.getHostAddress().equals(listener.getClusterHostAddress())) {
                        listener.retrieveAllEndpointsAndSet(protocol);
                        if (listener.getUrlParser().getHostAddresses().size() > 2) {
                            searchFilter = new SearchFilter(false);
                            loopAddresses.addAll(listener.getUrlParser().getHostAddresses());
                        }
                    }
                    if (foundMaster(listener, protocol, searchFilter)) {
                        return;
                    }

                } else if (!protocol.isMasterConnection()) {
                    if (listener.isSecondaryHostFailReconnect()) {
                        if (foundSecondary(listener, protocol, searchFilter)) {
                            return;
                        }
                    }

                    if (listener.isSecondaryHostFailReconnect()
                            || (listener.isMasterHostFailReconnect() && probableMasterHost == null)) {
                        probableMasterHost = listener.searchByStartName(protocol, listener.getUrlParser().getHostAddresses());
                        if (probableMasterHost != null) {
                            loopAddresses.remove(probableMasterHost);
                            AuroraProtocol.searchProbableMaster(listener, probableMasterHost);
                            if (listener.isMasterHostFailReconnect() && searchFilter.isFineIfFoundOnlySlave()) {
                                return;
                            }
                        }
                    }
                } else {
                    protocol.close();
                }

            } catch (QueryException e) {
                lastQueryException = e;
                listener.addToBlacklist(protocol.getHostAddress());
            }

            if (!listener.isMasterHostFailReconnect() && !listener.isSecondaryHostFailReconnect()) {
                return;
            }

            //loop is set so
            if (loopAddresses.isEmpty() && !searchFilter.isFailoverLoop() && maxConnectionTry > 0) {
                //use blacklist if all server has been connected and no result
                loopAddresses = new ArrayDeque<>(listener.getBlacklistKeys());
            }

            // Try to connect to the cluster if no other connection is good
            if (maxConnectionTry == 0 && !loopAddresses.contains(listener.getClusterHostAddress()) && listener.getClusterHostAddress() != null) {
                loopAddresses.add(listener.getClusterHostAddress());
            }

        }

        if (listener.isMasterHostFailReconnect() || listener.isSecondaryHostFailReconnect()) {
            String error = "No active connection found for replica";
            if (listener.isMasterHostFailReconnect())  {
                error = "No active connection found for master";
            }
            if (lastQueryException != null) {
                throw new QueryException(error, lastQueryException.getErrorCode(), lastQueryException.getSqlState(), lastQueryException);
            }
            throw new QueryException(error);
        }
    }

    /**
     * Initialize new protocol instance.
     * @param proxy proxy
     * @param urlParser connection string data's
     * @return new AuroraProtocol
     */
    public static AuroraProtocol getNewProtocol(FailoverProxy proxy, UrlParser urlParser) {
        AuroraProtocol newProtocol = new AuroraProtocol(urlParser, proxy.lock);
        newProtocol.setProxy(proxy);
        return newProtocol;
    }

    @Override
    public boolean isMasterConnection() {
        return this.masterConnection;
    }

    /**
     * Aurora best way to check if a node is a master : is not in read-only mode.
     *
     * @return indicate if master has been found
     */
    @Override
    public boolean checkIfMaster() throws QueryException {
        proxy.lock.lock();
        try {
            SingleExecutionResult executionResult = new SingleExecutionResult(null, 0, true, false);
            executeQuery(executionResult, "show global variables like 'innodb_read_only'", ResultSet.TYPE_FORWARD_ONLY);
            MariaSelectResultSet queryResult = executionResult.getResult();
            if (queryResult != null) {
                queryResult.next();
                this.masterConnection = "OFF".equals(queryResult.getString(2));
            } else {
                this.masterConnection = false;
            }
            this.readOnly = !this.masterConnection;
            return this.masterConnection;

        } catch (SQLException sqle) {
            throw new QueryException("could not check the 'innodb_read_only' variable status on " + this.getHostAddress()
                    + " : " + sqle.getMessage(), -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState(), sqle);
        } finally {
            proxy.lock.unlock();
        }
    }


}
