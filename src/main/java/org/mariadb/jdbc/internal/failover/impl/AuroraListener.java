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
import org.mariadb.jdbc.internal.queryresults.SingleExecutionResult;
import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.protocol.AuroraProtocol;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.dao.ReconnectDuringTransactionException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class AuroraListener extends MastersSlavesListener {
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

        if (!isSecondaryHostFail()) {
            if (secondaryProtocol != null) {
                loopAddress.remove(secondaryProtocol.getHostAddress());
                loopAddress.add(secondaryProtocol.getHostAddress());
            }
        }

        if ((isMasterHostFail() || isSecondaryHostFail())
                || searchFilter.isInitialConnection()) {
            //while permit to avoid case when succeeded creating a new Master connection
            //and ping master connection fail a few millissecond after,
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
    }


    /**
     * Aurora replica doesn't have the master endpoint but the master instance name.
     * since the end point normally use the instance name like "instancename.some_ugly_string.region.rds.amazonaws.com",
     * if an endpoint start with this instance name, it will be checked first.
     *
     * @param secondaryProtocol the current secondary protocol
     * @param loopAddress       list of possible hosts
     * @return the probable master address or null if not found
     */
    public HostAddress searchByStartName(Protocol secondaryProtocol, List<HostAddress> loopAddress) {
        if (!isSecondaryHostFail()) {
            MariaSelectResultSet queryResult = null;
            try {
                proxy.lock.lock();
                try {
                    SingleExecutionResult executionResult = new SingleExecutionResult(null, 0, true, false);
                    secondaryProtocol.executeQuery(executionResult,
                            "select server_id from information_schema.replica_host_status where session_id = 'MASTER_SESSION_ID'",
                            ResultSet.TYPE_FORWARD_ONLY);
                    queryResult = executionResult.getResult();
                    queryResult.next();
                } finally {
                    proxy.lock.unlock();
                }
                String masterHostName = queryResult.getString(1);
                for (int i = 0; i < loopAddress.size(); i++) {
                    if (loopAddress.get(i).host.startsWith(masterHostName)) {
                        return loopAddress.get(i);
                    }
                }
            } catch (SQLException exception) {
                //eat exception because cannot happen in this getString()
            } catch (QueryException qe) {
                if (proxy.hasToHandleFailover(qe) && setSecondaryHostFail()) {
                    addToBlacklist(currentProtocol.getHostAddress());
                }
            }
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
