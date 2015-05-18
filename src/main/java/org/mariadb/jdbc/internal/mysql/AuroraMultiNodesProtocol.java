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
import org.mariadb.jdbc.JDBCUrl;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.query.MySQLQuery;
import org.mariadb.jdbc.internal.common.queryresults.SelectQueryResult;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AuroraMultiNodesProtocol extends MultiNodesProtocol {

    public AuroraMultiNodesProtocol(JDBCUrl url,
                              final String username,
                              final String password,
                              Properties info) {
        super(url, username, password, info);
    }

    /**
     * Aurora best way to check if a node is a master : is not in read-only mode
     * @return
     * @throws SQLException
     */
    public boolean checkIfMaster() throws SQLException {
        try {

            SelectQueryResult queryResult = (SelectQueryResult) executeQuery(new MySQLQuery("show global variables like 'innodb_read_only'"));
            if (queryResult != null) {
                queryResult.next();
                masterConnection = "OFF".equals(queryResult.getValueObject(1).getString());
            } else {
                masterConnection = false;
            }
            return masterConnection;

        } catch(IOException ioe) {
            throw new SQLException(ioe);
        } catch (QueryException qe) {
            throw new SQLException(qe);
        }
    }

    /**
     * loop until found the failed connection.
     * search order :
     * - in the host without the failed ones
     * - the failed host if not found
     * @param listener
     * @param addrs
     * @param failAddress
     * @param searchForMaster
     * @param searchForSecondary
     * @throws QueryException
     */
    public void loop(AuroraHostListener listener, List<HostAddress> addrs, List<HostAddress> failAddress, boolean searchForMaster, boolean searchForSecondary) throws QueryException {
        List<HostAddress> verifiedAddrs = new ArrayList<HostAddress>();
        for(HostAddress host : addrs) {
            try {
                currentHost = host;
                verifiedAddrs.add(host);
                connect(currentHost.host, currentHost.port);
                if (searchForMaster && isMasterConnection()) {
                    searchForMaster = false;
                    listener.foundActiveMaster(this);
                    if (!searchForSecondary) return;
                    else {
                        loopInternal(listener, addrs, failAddress,  verifiedAddrs, searchForMaster, searchForSecondary);
                        return;
                    }
                }
                if (searchForSecondary &&  !isMasterConnection()) {
                    searchForSecondary = false;
                    listener.foundActiveSecondary(this);
                    if (!searchForMaster) return;
                    else {
                        loopInternal(listener, addrs, failAddress, verifiedAddrs, searchForMaster, searchForSecondary);
                        return;
                    }
                }

            } catch (QueryException e ) {
                log.finest("Could not connect to " + host +" : " + e.getMessage());
            } catch (SQLException e ) {
                log.finest("Could not connect to " + host +" : " + e.getMessage());
            } catch (IOException e ) {
                log.finest("Could not connect to " + host +" : " + e.getMessage());
            }
            if (!searchForMaster && !searchForSecondary) return;
        }


        if ((searchForMaster || searchForSecondary) && failAddress != null) {
            //the loop has trait all host but failed and not found what we want, so continue on failed Hosts
            loopInternal(listener, failAddress, null, null, searchForMaster, searchForSecondary);
        }

        if (searchForMaster || searchForSecondary) {
            throw new QueryException("No active connection found");
        }
    }

    /**
     * second loop when the 2 connections have fallen, to search for the remaining fallen connection
     * @param listener
     * @param addrs the list of remaining Host not verified
     * @param failAddress the list of the failed host
     * @param hostsToRemove the list of the host already verified
     * @param searchForMaster boolean to indicate if searching master
     * @param searchForSecondary boolean to indicate if search for a replicate
     * @throws QueryException
     */
    private void loopInternal(AuroraHostListener listener, List<HostAddress> addrs, List<HostAddress> failAddress, List<HostAddress> hostsToRemove, boolean searchForMaster, boolean searchForSecondary)  throws QueryException {
        AuroraMultiNodesProtocol newProtocol = new AuroraMultiNodesProtocol(this.jdbcUrl,
                this.getUsername(),
                this.getPassword(),
                this.getInfo());
        List<HostAddress> remainingAddrs = new ArrayList<HostAddress>(addrs);
        if (hostsToRemove != null) remainingAddrs.removeAll(hostsToRemove);
        newProtocol.loop(listener, remainingAddrs, failAddress, searchForMaster, searchForSecondary);
    }
}
