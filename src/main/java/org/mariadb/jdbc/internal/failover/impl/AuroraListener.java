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

package org.mariadb.jdbc.internal.failover.impl;

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.failover.tools.SearchFilter;
import org.mariadb.jdbc.internal.protocol.AuroraProtocol;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.queryresults.SingleExecutionResult;
import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.util.dao.ReconnectDuringTransactionException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AuroraListener extends MastersSlavesListener {

    private final Logger log = Logger.getLogger(AuroraListener.class.getName());
    private final Pattern clusterPattern = Pattern.compile("(.+)\\.cluster-([a-z0-9]+\\.[a-z0-9\\-]+\\.rds\\.amazonaws\\.com)");
    private final HostAddress clusterHostAddress;
    private final SimpleDateFormat sqlDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private String urlEndStr = "";
    private String dbName = "information_schema";

    /**
     * Constructor for Aurora.
     * This differ from standard failover because :
     * - we don't know current master, we must check that after initial connection
     * - master can change after he has a failover
     *
     * @param urlParser connection informations
     */
    public AuroraListener(UrlParser urlParser) {
        super(urlParser);
        masterProtocol = null;
        secondaryProtocol = null;
        clusterHostAddress = findClusterHostAddress(urlParser);
    }

    /**
     * Retrieves the cluster host address from the UrlParser instance.
     *
     * @param urlParser object that holds the connection information
     * @return cluster host address
     */
    private HostAddress findClusterHostAddress(UrlParser urlParser) {
        List<HostAddress> hostAddresses = urlParser.getHostAddresses();
        Matcher matcher;
        for (HostAddress hostAddress : hostAddresses) {
            matcher = clusterPattern.matcher(hostAddress.host);
            if (matcher.find()) {
                urlEndStr = "." + matcher.group(2);
                return hostAddress;
            }
        }
        return null;
    }

    public HostAddress getClusterHostAddress() {
        return clusterHostAddress;
    }

    /**
     * Search a valid connection for failed one.
     * A Node can be a master or a replica depending on the cluster state.
     * so search for each host until found all the failed connection.
     * By default, search for the host not down, and recheck the down one after if not found valid connections.
     *
     * @throws QueryException if a connection asked is not found
     */
    @Override
    public void reconnectFailedConnection(SearchFilter searchFilter) throws QueryException {
        if (!searchFilter.isInitialConnection()
                && (isExplicitClosed()
                || (searchFilter.isFineIfFoundOnlyMaster() && !isMasterHostFail())
                || searchFilter.isFineIfFoundOnlySlave() && !isSecondaryHostFail())) {
            return;
        }

        if (!searchFilter.isFailoverLoop()) {
            try {
                checkWaitingConnection();
                if ((searchFilter.isFineIfFoundOnlyMaster() && !isMasterHostFail())
                        || searchFilter.isFineIfFoundOnlySlave() && !isSecondaryHostFail()) {
                    return;
                }
            } catch (ReconnectDuringTransactionException e) {
                //don't throw an exception for this specific exception
                return;
            }
        }

        currentConnectionAttempts.incrementAndGet();

        resetOldsBlackListHosts();

        //put the list in the following order
        // - random order not connected host and not blacklisted
        // - random blacklisted host
        // - connected host at end.
        List<HostAddress> loopAddress = new LinkedList<>(urlParser.getHostAddresses());
        loopAddress.removeAll(getBlacklistKeys());
        Collections.shuffle(loopAddress);
        List<HostAddress> blacklistShuffle = new LinkedList<>(getBlacklistKeys());
        Collections.shuffle(blacklistShuffle);
        loopAddress.addAll(blacklistShuffle);

        //put connected at end
        if (masterProtocol != null && !isMasterHostFail()) {
            loopAddress.remove(masterProtocol.getHostAddress());
            loopAddress.add(masterProtocol.getHostAddress());
        }

        if (!isSecondaryHostFail() && secondaryProtocol != null) {
            loopAddress.remove(secondaryProtocol.getHostAddress());
            loopAddress.add(secondaryProtocol.getHostAddress());
        }

        if (urlParser.getHostAddresses().size() <= 1) {
            searchFilter = new SearchFilter(true, false);
        }
        if ((isMasterHostFail() || isSecondaryHostFail())
                || searchFilter.isInitialConnection()) {
            //while permit to avoid case when succeeded creating a new Master connection
            //and ping master connection fail a few milliseconds after,
            //resulting a masterConnection not initialized.
            do {
                AuroraProtocol.loop(this, loopAddress, searchFilter);
                if (!searchFilter.isFailoverLoop()) {
                    try {
                        checkWaitingConnection();
                    } catch (ReconnectDuringTransactionException e) {
                        //don't throw an exception for this specific exception
                    }
                }
            } while (searchFilter.isInitialConnection() && masterProtocol == null);
        }

        if (getCurrentProtocol() != null && !getCurrentProtocol().isClosed()) {
            retrieveAllEndpointsAndSet(getCurrentProtocol());
        }

    }

    /**
     * Retrieves the information necessary to add a new endpoint.
     * Calls the methods that retrieves the instance identifiers and sets urlParser accordingly.
     *
     * @param protocol current protocol connected to
     * @throws QueryException if connection error occur
     */
    public void retrieveAllEndpointsAndSet(Protocol protocol) throws QueryException {
        // For a given cluster, same port for all endpoints and same end host address
        int port = protocol.getPort();
        if ("".equals(urlEndStr) && protocol.getHost().indexOf(".") > -1) {
            urlEndStr = protocol.getHost().substring(protocol.getHost().indexOf("."));
        }

        List<String> endpoints = getCurrentEndpointIdentifiers(protocol);
        if (!"".equals(urlEndStr)) {
            setUrlParserFromEndpoints(endpoints, port);
        }

    }

    /**
     * Retrieves all endpoints of a cluster from the appropriate database table.
     *
     * @param protocol current protocol connected to
     * @return instance endpoints of the cluster
     * @throws QueryException if connection error occur
     */
    private List<String> getCurrentEndpointIdentifiers(Protocol protocol) throws QueryException {
        List<String> endpoints = new ArrayList<>();
        try {
            proxy.lock.lock();
            try {
                // Deleted instance may remain in db for 24 hours so ignoring instances that have had no change
                // for 3 minutes
                Date date = new Date();

                Timestamp currentTime = new Timestamp(date.getTime() - 3 * 60 * 1000); // 3 minutes
                sqlDateFormat.setTimeZone(TimeZone.getTimeZone(protocol.getServerData("system_time_zone")));

                SingleExecutionResult queryResult = new SingleExecutionResult(null, 0, true, false);
                protocol.executeQuery(false, queryResult,
                        "select server_id, session_id from " + dbName + ".replica_host_status "
                                + "where last_update_timestamp > '" + sqlDateFormat.format(currentTime) + "'",
                        ResultSet.TYPE_FORWARD_ONLY);
                MariaSelectResultSet resultSet = queryResult.getResultSet();

                while (resultSet.next()) {
                    endpoints.add(resultSet.getString(1) + urlEndStr);
                }

                //randomize order for distributed load-balancing
                Collections.shuffle(endpoints);

            } finally {
                proxy.lock.unlock();
            }
        } catch (SQLException se) {
            log.log(Level.WARNING, "SQL exception occurred: " + se);
        } catch (QueryException qe) {
            if (protocol.getProxy().hasToHandleFailover(qe)) {
                if (masterProtocol.equals(protocol)) {
                    setMasterHostFail();
                } else if (secondaryProtocol.equals(protocol)) {
                    setSecondaryHostFail();
                }
                addToBlacklist(protocol.getHostAddress());
                reconnectFailedConnection(new SearchFilter(isMasterHostFail(), isSecondaryHostFail()));
            }
        }

        return endpoints;
    }

    /**
     * Sets urlParser accordingly to discovered hosts.
     *
     * @param endpoints instance identifiers
     * @param port      port that is common to all endpoints
     */
    private void setUrlParserFromEndpoints(List<String> endpoints, int port) {
        List<HostAddress> addresses = new ArrayList<>();
        for (String endpoint : endpoints) {
            HostAddress newHostAddress = new HostAddress(endpoint, port, null);
            addresses.add(newHostAddress);
        }

        synchronized (urlParser) {
            urlParser.setHostAddresses(addresses);
        }
    }

    /**
     * Looks for the current master/writer instance via the secondary protocol if it is found within 3 attempts.
     * Should it not be able to connect, the host is blacklisted and null is returned.
     * Otherwise, it will open a new connection to the cluster endpoint and retrieve the data from there.
     *
     * @param secondaryProtocol the current secondary protocol
     * @param loopAddress       list of possible hosts
     * @return the probable master address or null if not found
     */
    public HostAddress searchByStartName(Protocol secondaryProtocol, List<HostAddress> loopAddress) {
        if (!isSecondaryHostFail()) {
            int checkWriterAttempts = 3;
            HostAddress currentWriter = null;

            do {
                try {
                    currentWriter = searchForMasterHostAddress(secondaryProtocol, loopAddress);
                } catch (QueryException qe) {
                    if (proxy.hasToHandleFailover(qe) && setSecondaryHostFail()) {
                        addToBlacklist(secondaryProtocol.getHostAddress());
                        return null;
                    }
                }
                checkWriterAttempts--;
            } while (currentWriter == null && checkWriterAttempts > 0);

            // Handling special case where no writer is found from secondaryProtocol
            if (currentWriter == null && getClusterHostAddress() != null) {
                AuroraProtocol possibleMasterProtocol = AuroraProtocol.getNewProtocol(getProxy(), getUrlParser());
                possibleMasterProtocol.setHostAddress(getClusterHostAddress());
                try {
                    possibleMasterProtocol.connect();
                    possibleMasterProtocol.setMustBeMasterConnection(true);
                    foundActiveMaster(possibleMasterProtocol);
                } catch (QueryException qe) {
                    if (proxy.hasToHandleFailover(qe)) {
                        addToBlacklist(possibleMasterProtocol.getHostAddress());
                    }
                }
            }

            return currentWriter;
        }
        return null;
    }

    /**
     * Aurora replica doesn't have the master endpoint but the master instance name.
     * since the end point normally use the instance name like "instance-name.some_unique_string.region.rds.amazonaws.com",
     * if an endpoint start with this instance name, it will be checked first.
     * Otherwise, the endpoint ending string is extracted and used since the writer was newly created.
     *
     * @param protocol    current protocol
     * @param loopAddress list of possible hosts
     * @return the probable host address or null if no valid endpoint found
     * @throws QueryException if any connection error occur
     */
    private HostAddress searchForMasterHostAddress(Protocol protocol, List<HostAddress> loopAddress) throws QueryException {
        String masterHostName = null;
        proxy.lock.lock();
        try {
            Date date = new Date();
            Timestamp currentTime = new Timestamp(date.getTime() - 3 * 60 * 1000); // 3 minutes
            sqlDateFormat.setTimeZone(TimeZone.getTimeZone(protocol.getServerData("system_time_zone")));

            SingleExecutionResult executionResult = new SingleExecutionResult(null, 0, true, false);
            protocol.executeQuery(false, executionResult,
                    "select server_id from " + dbName + ".replica_host_status "
                            + "where session_id = 'MASTER_SESSION_ID' "
                            + "and last_update_timestamp = ("
                            + "select max(last_update_timestamp) from " + dbName + ".replica_host_status "
                            + "where session_id = 'MASTER_SESSION_ID' "
                            + "and last_update_timestamp > '" + currentTime + "')",
                    ResultSet.TYPE_FORWARD_ONLY);
            MariaSelectResultSet queryResult = executionResult.getResultSet();

            if (!queryResult.isBeforeFirst()) {
                return null;
            } else {
                queryResult.next();
                masterHostName = queryResult.getString(1);
            }

        } catch (SQLException sqle) {
            //eat exception because cannot happen in this getString()
        } finally {
            proxy.lock.unlock();
        }

        Matcher matcher;
        if (masterHostName != null) {
            for (HostAddress hostAddress : loopAddress) {
                matcher = clusterPattern.matcher(hostAddress.host);
                if (hostAddress.host.startsWith(masterHostName) && !matcher.find()) {
                    return hostAddress;
                }
            }

            HostAddress masterHostAddress;
            if (urlEndStr.equals("") && protocol.getHost().indexOf(".") > -1) {
                urlEndStr = protocol.getHost().substring(protocol.getHost().indexOf("."));
            } else {
                return null;
            }

            masterHostAddress = new HostAddress(masterHostName + urlEndStr, protocol.getPort(), null);
            loopAddress.add(masterHostAddress);
            urlParser.setHostAddresses(loopAddress);
            return masterHostAddress;
        }

        return null;
    }

    @Override
    public boolean checkMasterStatus(SearchFilter searchFilter) {
        if (!isMasterHostFail()) {
            try {
                if (masterProtocol != null && !masterProtocol.checkIfMaster()) {
                    //master has been demote, is now secondary
                    setMasterHostFail();
                    if (isSecondaryHostFail()) {
                        foundActiveSecondary(masterProtocol);
                    }
                    return true;
                }
            } catch (QueryException e) {
                try {
                    masterProtocol.ping();
                } catch (QueryException ee) {
                    proxy.lock.lock();
                    try {
                        masterProtocol.close();
                    } finally {
                        proxy.lock.unlock();
                    }
                    if (setMasterHostFail()) {
                        addToBlacklist(masterProtocol.getHostAddress());
                    }
                }
                return true;
            }
        }

        if (!isSecondaryHostFail()) {
            try {
                if (secondaryProtocol != null && secondaryProtocol.checkIfMaster()) {
                    //secondary has been promoted to master
                    setSecondaryHostFail();
                    if (isMasterHostFail()) {
                        foundActiveMaster(secondaryProtocol);
                    }
                    return true;
                }
            } catch (QueryException e) {
                try {
                    this.secondaryProtocol.ping();
                } catch (Exception ee) {
                    proxy.lock.lock();
                    try {
                        secondaryProtocol.close();
                    } finally {
                        proxy.lock.unlock();
                    }
                    if (setSecondaryHostFail()) {
                        addToBlacklist(this.secondaryProtocol.getHostAddress());
                    }
                    return true;
                }
            }
        }

        return false;
    }

}
