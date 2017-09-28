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
import org.mariadb.jdbc.internal.com.read.dao.Results;
import org.mariadb.jdbc.internal.failover.FailoverProxy;
import org.mariadb.jdbc.internal.failover.impl.AuroraListener;
import org.mariadb.jdbc.internal.failover.tools.SearchFilter;

import java.net.SocketException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import static org.mariadb.jdbc.internal.util.SqlStates.CONNECTION_EXCEPTION;

public class AuroraProtocol extends MastersSlavesProtocol {

    public AuroraProtocol(final UrlParser url, final ReentrantLock lock) {
        super(url, lock);
    }

    /**
     * Connect aurora probable master.
     * Aurora master change in time. The only way to check that a server is a master is to asked him.
     *
     * @param listener       aurora failover to call back if master is found
     * @param probableMaster probable master host
     */
    private static void searchProbableMaster(AuroraListener listener, HostAddress probableMaster) {
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

        } catch (SQLException e) {
            listener.addToBlacklist(protocol.getHostAddress());
        }
    }

    /**
     * loop until found the failed connection.
     *
     * @param listener              current failover
     * @param addresses             list of HostAddress to loop
     * @param initialSearchFilter   search parameter
     * @throws SQLException if not found
     */
    public static void loop(AuroraListener listener, final List<HostAddress> addresses, SearchFilter initialSearchFilter)
            throws SQLException {

        SearchFilter searchFilter = initialSearchFilter;
        AuroraProtocol protocol;
        Deque<HostAddress> loopAddresses = new ArrayDeque<HostAddress>(addresses);
        if (loopAddresses.isEmpty()) resetHostList(listener, loopAddresses);

        int maxConnectionTry = listener.getRetriesAllDown();
        SQLException lastQueryException = null;
        HostAddress probableMasterHost = null;

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
                    if (listener.getClusterHostAddress() != null && listener.getUrlParser().getHostAddresses().size() < 2) {
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
                    if (searchFilter.isFineIfFoundOnlyMaster() && listener.getUrlParser().getHostAddresses().size() <= 1
                            && protocol.getHostAddress().equals(listener.getClusterHostAddress())) {
                        listener.retrieveAllEndpointsAndSet(protocol);

                        if (listener.getUrlParser().getHostAddresses().size() > 1) {
                            //add newly discovered end-point to loop
                            loopAddresses.addAll(listener.getUrlParser().getHostAddresses());
                            //since there is more than one end point, reactivate connection to a read-only host
                            searchFilter = new SearchFilter(false);
                        }
                    }

                    if (foundMaster(listener, protocol, searchFilter)) {
                        return;
                    }

                } else if (!protocol.isMasterConnection()) {
                    if (listener.isSecondaryHostFailReconnect()) {
                        //in case cluster DNS is currently pointing to a slave host
                        if (listener.getUrlParser().getHostAddresses().size() <= 1
                                && protocol.getHostAddress().equals(listener.getClusterHostAddress())) {
                            listener.retrieveAllEndpointsAndSet(protocol);

                            if (listener.getUrlParser().getHostAddresses().size() > 1) {
                                //add newly discovered end-point to loop
                                loopAddresses.addAll(listener.getUrlParser().getHostAddresses());
                                //since there is more than one end point, reactivate connection to a read-only host
                                searchFilter = new SearchFilter(false);
                            }
                        } else {
                            if (foundSecondary(listener, protocol, searchFilter)) {
                                return;
                            }
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

            // Try to connect to the cluster if no other connection is good
            if (maxConnectionTry == 0 && !loopAddresses.contains(listener.getClusterHostAddress()) && listener.getClusterHostAddress() != null) {
                loopAddresses.add(listener.getClusterHostAddress());
            }

        }

        if (listener.isMasterHostFailReconnect() || listener.isSecondaryHostFailReconnect()) {
            String error = "No active connection found for replica";
            if (listener.isMasterHostFailReconnect()) {
                error = "No active connection found for master";
            }
            if (lastQueryException != null) {
                throw new SQLException(error, lastQueryException.getSQLState(), lastQueryException.getErrorCode(), lastQueryException);
            }
            throw new SQLException(error);
        }
    }

    /**
     * Reinitialize loopAddresses with all hosts : all servers in randomize order with cluster address.
     * If there is an active connection, connected host are remove from list.
     *
     * @param listener      current listener
     * @param loopAddresses the list to reinitialize
     */
    private static void resetHostList(AuroraListener listener, Deque<HostAddress> loopAddresses) {
        //if all servers have been connected without result
        //add back all servers
        List<HostAddress> servers = new ArrayList<HostAddress>();
        servers.addAll(listener.getUrlParser().getHostAddresses());

        Collections.shuffle(servers);

        //if cluster host is set, add it to the end of the list
        if (listener.getClusterHostAddress() != null && listener.getUrlParser().getHostAddresses().size() < 2) {
            servers.add(listener.getClusterHostAddress());
        }

        //remove current connected hosts to avoid reconnect them
        servers.removeAll(listener.connectedHosts());

        loopAddresses.clear();
        loopAddresses.addAll(servers);
    }

    /**
     * Initialize new protocol instance.
     *
     * @param proxy     proxy
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

    @Override
    public void readPipelineCheckMaster() throws SQLException {
        Results results = new Results();
        getResult(results);
        results.commandEnd();
        ResultSet resultSet = results.getResultSet();

        if (!resultSet.next()) throw new SQLException("Error checking Aurora's master status : No information");

        this.masterConnection = "OFF".equals(resultSet.getString(2));
        reader.setServerThreadId(this.serverThreadId, this.masterConnection);
        writer.setServerThreadId(this.serverThreadId, this.masterConnection);
        //Aurora replicas have read-only flag forced
        this.readOnly = !this.masterConnection;

    }

    @Override
    public boolean isValid(int timeout) throws SQLException {

        try {

            this.socket.setSoTimeout(timeout);

            if (isMasterConnection()) return checkIfMaster();
            return ping();

        } catch (SocketException socketException) {
            throw new SQLException("Could not valid connection : " + socketException.getMessage(),
                    CONNECTION_EXCEPTION.getSqlState(),
                    socketException);
        } finally {

            //set back initial socket timeout
            try {
                this.socket.setSoTimeout(options.socketTimeout == null ? 0 : options.socketTimeout);
            } catch (SocketException socketException) {
                //eat
            }
        }
    }

    /**
     * Aurora best way to check if a node is a master : is not in read-only mode.
     *
     * @return indicate if master has been found
     */
    @Override
    public boolean checkIfMaster() throws SQLException {
        proxy.lock.lock();
        try {
            Results results = new Results();
            executeQuery(this.isMasterConnection(), results, "show global variables like 'innodb_read_only'");
            results.commandEnd();
            ResultSet queryResult = results.getResultSet();
            if (queryResult != null) {
                queryResult.next();
                this.masterConnection = "OFF".equals(queryResult.getString(2));

                reader.setServerThreadId(this.serverThreadId, this.masterConnection);
                writer.setServerThreadId(this.serverThreadId, this.masterConnection);
            } else {
                this.masterConnection = false;
            }

            this.readOnly = !this.masterConnection;
            return this.masterConnection;

        } catch (SQLException sqle) {
            throw new SQLException("could not check the 'innodb_read_only' variable status on " + this.getHostAddress()
                    + " : " + sqle.getMessage(), CONNECTION_EXCEPTION.getSqlState(), sqle);
        } finally {
            proxy.lock.unlock();
        }
    }


}
