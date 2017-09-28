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
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.failover.FailoverProxy;
import org.mariadb.jdbc.internal.failover.impl.MastersSlavesListener;
import org.mariadb.jdbc.internal.failover.tools.SearchFilter;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class MastersSlavesProtocol extends MasterProtocol {
    protected boolean masterConnection = false;
    private boolean mustBeMasterConnection = false;

    public MastersSlavesProtocol(final UrlParser url, final ReentrantLock lock) {
        super(url, lock);
    }


    /**
     * loop until found the failed connection.
     *
     * @param listener     current failover
     * @param addresses    list of HostAddress to loop
     * @param searchFilter search parameter
     * @throws SQLException if not found
     */
    public static void loop(MastersSlavesListener listener, final List<HostAddress> addresses,
                            SearchFilter searchFilter) throws SQLException {

        MastersSlavesProtocol protocol;
        ArrayDeque<HostAddress> loopAddresses = new ArrayDeque<HostAddress>(addresses);
        if (loopAddresses.isEmpty()) resetHostList(listener, loopAddresses);

        int maxConnectionTry = listener.getRetriesAllDown();
        SQLException lastQueryException = null;

        while (!loopAddresses.isEmpty() || (!searchFilter.isFailoverLoop() && maxConnectionTry > 0)) {
            protocol = getNewProtocol(listener.getProxy(), listener.getUrlParser());

            if (listener.isExplicitClosed() || (!listener.isSecondaryHostFailReconnect() && !listener.isMasterHostFailReconnect())) {
                return;
            }
            maxConnectionTry--;

            try {
                HostAddress host = loopAddresses.pollFirst();
                if (host == null) {
                    loopAddresses.addAll(listener.getUrlParser().getHostAddresses());
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
                    if (foundMaster(listener, protocol, searchFilter)) {
                        return;
                    }
                } else if (listener.isSecondaryHostFailReconnect() && !protocol.isMasterConnection()) {
                    if (foundSecondary(listener, protocol, searchFilter)) {
                        return;
                    }
                } else {
                    protocol.close();
                }

            } catch (SQLException e) {
                lastQueryException = e;
                listener.addToBlacklist(protocol.getHostAddress());
            }

            if (!listener.isMasterHostFailReconnect() && !listener.isSecondaryHostFailReconnect()) {
                return;
            }

            // if server has try to connect to all host, and there is remaining master or slave that fail
            // add all servers back to continue looping until maxConnectionTry is reached
            if (loopAddresses.isEmpty() && !searchFilter.isFailoverLoop() && maxConnectionTry > 0) {
                resetHostList(listener, loopAddresses);
            }
        }

        if (listener.isMasterHostFailReconnect() || listener.isSecondaryHostFailReconnect()) {
            String error = "No active connection found for replica";
            if (listener.isMasterHostFailReconnect()) {
                error = "No active connection found for master";
            }
            if (lastQueryException != null) {
                throw new SQLException(error + " : " + lastQueryException.getMessage(),
                        lastQueryException.getSQLState(), lastQueryException.getErrorCode(), lastQueryException);
            }
            throw new SQLException(error);
        }

    }

    /**
     * Reinitialize loopAddresses with all servers in randomize order.
     *
     * @param listener      current listener
     * @param loopAddresses the list to reinitialize
     */
    private static void resetHostList(MastersSlavesListener listener, Deque<HostAddress> loopAddresses) {
        //if all servers have been connected without result
        //add back all servers
        List<HostAddress> servers = new ArrayList<HostAddress>();
        servers.addAll(listener.getUrlParser().getHostAddresses());
        Collections.shuffle(servers);

        //remove current connected hosts to avoid reconnect them
        servers.removeAll(listener.connectedHosts());

        loopAddresses.clear();
        loopAddresses.addAll(servers);
    }

    protected static boolean foundMaster(MastersSlavesListener listener, MastersSlavesProtocol protocol,
                                         SearchFilter searchFilter) {
        protocol.setMustBeMasterConnection(true);
        if (listener.isMasterHostFailReconnect()) {
            listener.foundActiveMaster(protocol);
        } else {
            protocol.close();
        }

        if (!listener.isSecondaryHostFailReconnect()) {
            return true;
        } else {
            if (listener.isExplicitClosed()
                    || searchFilter.isFineIfFoundOnlyMaster()
                    || !listener.isSecondaryHostFailReconnect()) {
                return true;
            }
        }
        return false;
    }

    protected static boolean foundSecondary(MastersSlavesListener listener, MastersSlavesProtocol protocol,
                                            SearchFilter searchFilter) throws SQLException {
        protocol.setMustBeMasterConnection(false);
        if (listener.isSecondaryHostFailReconnect()) {
            listener.foundActiveSecondary(protocol);
        } else {
            protocol.close();
        }

        if (!listener.isMasterHostFailReconnect()) {
            return true;
        } else {
            if (listener.isExplicitClosed()
                    || searchFilter.isFineIfFoundOnlySlave()
                    || !listener.isMasterHostFailReconnect()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get new instance of MastersSlavesProtocol.
     *
     * @param proxy     proxy
     * @param urlParser connection string Object.
     * @return a new MastersSlavesProtocol instance
     */
    private static MastersSlavesProtocol getNewProtocol(FailoverProxy proxy, UrlParser urlParser) {
        MastersSlavesProtocol newProtocol = new MastersSlavesProtocol(urlParser, proxy.lock);
        newProtocol.setProxy(proxy);
        return newProtocol;
    }

    public boolean mustBeMasterConnection() {
        return mustBeMasterConnection;
    }

    public void setMustBeMasterConnection(boolean mustBeMasterConnection) {
        this.mustBeMasterConnection = mustBeMasterConnection;
    }
}
