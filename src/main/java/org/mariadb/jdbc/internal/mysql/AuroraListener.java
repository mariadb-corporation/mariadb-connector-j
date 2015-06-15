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

package org.mariadb.jdbc.internal.mysql;

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.query.MySQLQuery;
import org.mariadb.jdbc.internal.common.queryresults.QueryResult;
import org.mariadb.jdbc.internal.common.queryresults.ResultSetType;
import org.mariadb.jdbc.internal.common.queryresults.SelectQueryResult;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class AuroraListener extends MultiHostListener {
    private final static Logger log = Logger.getLogger(AuroraListener.class.getName());

    public AuroraListener() {
        masterProtocol = null;
        secondaryProtocol = null;
    }

    @Override
    public void initializeConnection(Protocol protocol) throws QueryException, SQLException {
        this.masterProtocol = (MultiNodesProtocol)protocol;
        this.currentProtocol = this.masterProtocol;
        parseHAOptions(protocol);
        if (validConnectionTimeout != 0)
            scheduledPing = Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new PingLoop(this), validConnectionTimeout, validConnectionTimeout, TimeUnit.SECONDS);
        try {
            reconnectFailedConnection(true, true, true);
        } catch (QueryException e) {
            checkInitialConnection();
            throw e;
        } catch (SQLException e) {
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
     * @throws QueryException
     * @throws SQLException
     */
    @Override
    public synchronized void reconnectFailedConnection(boolean searchForMaster, boolean searchForSecondary, boolean initialConnection) throws QueryException, SQLException {
        log.info("reconnectFailedConnection : searchForMaster="+searchForMaster+" searchForSecondary="+searchForSecondary);
        AuroraMultiNodesProtocol newProtocol = new AuroraMultiNodesProtocol(this.masterProtocol.jdbcUrl);
        newProtocol.setProxy(proxy);
        List<HostAddress> loopAddress = new LinkedList(Arrays.asList(this.masterProtocol.jdbcUrl.getHostAddresses()));
        List<HostAddress> failAddress = new LinkedList<HostAddress>();

        if (masterProtocol!=null && masterProtocol.getHostAddress() != null)loopAddress.remove(masterProtocol.currentHost);
        if (isMasterHostFail()) {
            failAddress.add(masterProtocol.currentHost);
        }

        if (secondaryProtocol!=null && secondaryProtocol.getHostAddress() != null)loopAddress.remove(secondaryProtocol.currentHost);
        if (isSecondaryHostFail()) {
            failAddress.add(secondaryProtocol.currentHost);
        } else searchByStartName(secondaryProtocol, loopAddress);

        if (((searchForMaster && isMasterHostFail())|| (searchForSecondary && isSecondaryHostFail())) || initialConnection) {
            newProtocol.loop(this, loopAddress, failAddress, new Boolean[]{searchForMaster, searchForSecondary});
        }
    }

    @Override
    public synchronized HandleErrorResult secondaryFail(Method method, Object[] args) throws Throwable {

        if (!isMasterHostFail()) {
            try {
                this.masterProtocol.ping(); //check that master is on before switching to him

                //since replica are restarted after a change of master, checking if the master as stayed master
                if (!masterProtocol.checkIfMaster()) {
                    log.finest("switching to new secondary connection");
                    syncConnection(this.secondaryProtocol, this.masterProtocol);
                    this.secondaryProtocol = this.masterProtocol;
                    setMasterHostFail();
                    currentProtocol = this.masterProtocol;
                } else {
                    log.finest("switching to master connection");
                    syncConnection(this.secondaryProtocol, this.masterProtocol);
                    currentProtocol = this.masterProtocol;
                }
                launchFailLoopIfNotlaunched(true); //launch reconnection loop to find the new master
                return relaunchOperation(method, args); //now that we are on master, relaunched result if the result was not crashing the master
            } catch (Exception e) {
                setMasterHostFail();
            }
        }

        if (autoReconnect) {
            try {
                reconnectFailedConnection();
                log.finest("SQL Primary node [" + this.masterProtocol.currentHost.toString() + "] connection re-established");
                return relaunchOperation(method, args); //now that we are reconnect, relaunched result if the result was not crashing the node
            } catch (Exception ee) {
                //in case master is down and another slave has been found
                if (!isSecondaryHostFail()) {
                    return relaunchOperation(method, args);
                }
                launchFailLoopIfNotlaunched(false);
                return new HandleErrorResult();
            }
        }
        launchFailLoopIfNotlaunched(true);
        return new HandleErrorResult();
    }

    /**
     * Aurora replica doesn't have the master endpoint but the master instance name.
     * since the end point normally use the instance name like "instancename.some_ugly_string.region.rds.amazonaws.com", if an endpoint start with this instance name, it will be checked first.
     *
     * @param loopAddress
     */
    protected void searchByStartName(Protocol secondaryProtocol, List<HostAddress> loopAddress) {
        try {
            QueryResult queryResult = secondaryProtocol.executeQuery(new MySQLQuery("select server_id from information_schema.replica_host_status where session_id = 'MASTER_SESSION_ID'"));
            if (queryResult.getResultSetType() == ResultSetType.SELECT && ((SelectQueryResult) queryResult).next()) {
                String masterHostName = ((SelectQueryResult) queryResult).getValueObject(0).getString();
                for (int i = 0; i < loopAddress.size(); i++) {
                    if (loopAddress.get(i).host.startsWith(masterHostName)) {
                        log.fine("master probably "+loopAddress.get(i));
                        if (i == 0) return;
                        else {
                            HostAddress probableMaster = loopAddress.get(i);
                            loopAddress.remove(i);
                            loopAddress.add(0, probableMaster);
                            return;
                        }
                    }
                }
            }
        } catch (Exception ioe) {
            //do nothing
        }
    }

}
