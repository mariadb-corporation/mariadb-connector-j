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

package org.mariadb.jdbc.internal.mysql.listener.impl;

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.JDBCUrl;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.query.MySQLQuery;
import org.mariadb.jdbc.internal.common.queryresults.SelectQueryResult;
import org.mariadb.jdbc.internal.mysql.*;
import org.mariadb.jdbc.internal.mysql.listener.tools.SearchFilter;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AuroraListener extends MastersSlavesListener {

    public AuroraListener(JDBCUrl jdbcUrl) {
        super(jdbcUrl);
        masterProtocol = null;
        secondaryProtocol = null;
        lastQueryTime = System.currentTimeMillis();
    }

    @Override
    public void initializeConnection() throws QueryException {
        if (jdbcUrl.getOptions().validConnectionTimeout != 0)
            scheduledPing = executorService.scheduleWithFixedDelay(new PingLoop(this), jdbcUrl.getOptions().validConnectionTimeout, jdbcUrl.getOptions().validConnectionTimeout, TimeUnit.SECONDS);
        try {
            reconnectFailedConnection(new SearchFilter(true, true, true));
        } catch (QueryException e) {
            checkInitialConnection();
            throw e;
        }
    }

    /**
     * search a valid connection for failed one.
     * A Node can be a master or a replica depending on the cluster state.
     * so search for each host until found all the failed connection.
     * By default, search for the host not down, and recheck the down one after if not found valid connections.
     *
     * @throws QueryException if a connection asked is not found
     */
    @Override
    public void reconnectFailedConnection(SearchFilter searchFilter) throws QueryException {
        currentConnectionAttempts.incrementAndGet();
        resetOldsBlackListHosts();

        //put the list in the following order
        // - random order not connected host
        // - random order blacklist host
        // - random order connected host
        List<HostAddress> loopAddress = new LinkedList<>(jdbcUrl.getHostAddresses());
        loopAddress.removeAll(blacklist.keySet());
        Collections.shuffle(loopAddress);
        List<HostAddress> blacklistShuffle = new LinkedList<>(blacklist.keySet());
        Collections.shuffle(blacklistShuffle);
        loopAddress.addAll(blacklistShuffle);

        //put connected at end
        if (masterProtocol != null && !isMasterHostFail()) {
            loopAddress.remove(masterProtocol.getHostAddress());
            //loopAddress.add(masterProtocol.getHostAddress());
        }

        if (!isSecondaryHostFail()) {
            if (secondaryProtocol != null) {
                loopAddress.remove(secondaryProtocol.getHostAddress());
                //loopAddress.add(secondaryProtocol.getHostAddress());
            }
            if (isMasterHostFail()) {
                HostAddress probableMaster = searchByStartName(secondaryProtocol, loopAddress);

                if (probableMaster != null) {
                    loopAddress.remove(probableMaster);
                    loopAddress.add(0, probableMaster);
                }
            }
        }

        if (((searchFilter.isSearchForMaster() && isMasterHostFail())|| (searchFilter.isSearchForSlave() && isSecondaryHostFail())) || searchFilter.isInitialConnection()) {
            AuroraProtocol.loop(this, loopAddress, blacklist, searchFilter);
        }
    }


    /**
     * Aurora replica doesn't have the master endpoint but the master instance name.
     * since the end point normally use the instance name like "instancename.some_ugly_string.region.rds.amazonaws.com", if an endpoint start with this instance name, it will be checked first.
     * @return the probable master address or null if not found
     * @param secondaryProtocol the current secondary protocol
     * @param loopAddress list of possible hosts
     */
    public HostAddress searchByStartName(Protocol secondaryProtocol, List<HostAddress> loopAddress) {
        if (!isSecondaryHostFail()) {
            SelectQueryResult queryResult = null;
            try {
                proxy.lock.writeLock().lock();
                try {
                    queryResult = (SelectQueryResult) secondaryProtocol.executeQuery(new MySQLQuery("select server_id from information_schema.replica_host_status where session_id = 'MASTER_SESSION_ID'"));
                    queryResult.next();
                } finally {
                    proxy.lock.writeLock().unlock();
                }
                String masterHostName = queryResult.getValueObject(0).getString();
                for (int i = 0; i < loopAddress.size(); i++) {
                    if (loopAddress.get(i).host.startsWith(masterHostName)) {
                        return loopAddress.get(i);
                    }
                }
            } catch (IOException ioe) {
                //eat exception
            } catch (QueryException qe) {
                if (proxy.hasToHandleFailover(qe)) {
                    if (setSecondaryHostFail()) {
                        addToBlacklist(currentProtocol.getHostAddress());
                    }
                }
            } finally {
                if (queryResult != null) {
                    queryResult.close();
                }
            }
        }
        return null;
    }

    @Override
    public void checkIfTypeHaveChanged(SearchFilter searchFilter) throws QueryException {
        if (!isMasterHostFail()) {
            try {
                if (masterProtocol != null && !masterProtocol.checkIfMaster()) {
                    //master has been demote, is now secondary
                    setMasterHostFail();
                    if (isSecondaryHostFail()) foundActiveSecondary(masterProtocol);
                    if (searchFilter != null) searchFilter.setSearchForSlave(false);
                    launchFailLoopIfNotlaunched(false);
                }
            } catch (QueryException e) {
                try {
                    masterProtocol.ping();
                } catch (QueryException ee) {
                    proxy.lock.writeLock().lock();
                    try {
                        masterProtocol.close();
                    } finally {
                        proxy.lock.writeLock().unlock();
                    }
                    if (setMasterHostFail()) addToBlacklist(masterProtocol.getHostAddress());
                }
                launchFailLoopIfNotlaunched(false);
            }
        }

        if (!isSecondaryHostFail()) {
            try {
                if (secondaryProtocol != null && secondaryProtocol.checkIfMaster()) {
                    //secondary has been promoted to master
                    setSecondaryHostFail();
                    if (isMasterHostFail()) foundActiveMaster(secondaryProtocol);
                    if (searchFilter != null) searchFilter.setSearchForMaster(false);
                    launchFailLoopIfNotlaunched(false);
                }
            } catch (QueryException e) {
                try {
                    this.secondaryProtocol.ping();
                } catch (Exception ee) {
                    proxy.lock.writeLock().lock();
                    try {
                        secondaryProtocol.close();
                    } finally {
                        proxy.lock.writeLock().unlock();
                    }
                    if (setSecondaryHostFail()) addToBlacklist(this.secondaryProtocol.getHostAddress());
                    launchFailLoopIfNotlaunched(false);
                }

            }
        }
    }

}
