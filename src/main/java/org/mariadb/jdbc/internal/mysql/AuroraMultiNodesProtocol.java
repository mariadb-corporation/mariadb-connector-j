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
import org.mariadb.jdbc.internal.SQLExceptionMapper;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.query.MySQLQuery;
import org.mariadb.jdbc.internal.common.queryresults.QueryResult;
import org.mariadb.jdbc.internal.common.queryresults.ResultSetType;
import org.mariadb.jdbc.internal.common.queryresults.SelectQueryResult;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AuroraMultiNodesProtocol extends MultiNodesProtocol {
    protected final static Logger log = Logger.getLogger(AuroraMultiNodesProtocol.class.getName());
    public AuroraMultiNodesProtocol(JDBCUrl url,
                              final String username,
                              final String password,
                              Properties info) {
        super(url, username, password, info);
    }

    /**
     * Aurora best way to check if a node is a master : is not in read-only mode
     * @return
     */
    @Override
    public boolean checkIfMaster() throws QueryException {
        try {

            SelectQueryResult queryResult = (SelectQueryResult) executeQuery(new MySQLQuery("show global variables like 'innodb_read_only'"));
            if (queryResult != null) {
                queryResult.next();
                this.masterConnection = "OFF".equals(queryResult.getValueObject(1).getString());
            } else {
                this.masterConnection = false;
            }
            return this.masterConnection;

        } catch(IOException ioe) {
            throw new QueryException("could not check the 'innodb_read_only' variable status on " + this.getHostAddress() +
                    " : " + ioe.getMessage(),  -1,  SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), ioe);
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
     * @param searchType
     * @throws QueryException
     */
    public void loop(AuroraListener listener, List<HostAddress> addrs, List<HostAddress> failAddress, Boolean[] searchType) throws QueryException {
        if (log.isLoggable(Level.FINE)) {
            log.fine("searching for master:"+ searchType[0]+ " replica:"+ searchType[0]+ " address:"+addrs+" failAddress:"+failAddress);
        }
        AuroraMultiNodesProtocol protocol = this;
        searchForProtocol(protocol, listener, addrs, searchType);

        if (searchType[0] || searchType[1]) {
            searchForProtocol(protocol, listener, failAddress, searchType);
        }
        if (searchType[0] || searchType[1]) {
            if (searchType[0]) throw new QueryException("No active connection found for master");
            else throw new QueryException("No active connection found for replica");
        }
    }

    private void searchForProtocol(AuroraMultiNodesProtocol protocol, AuroraListener listener, List<HostAddress> addresses, Boolean[] searchType) throws QueryException {
        while (!addresses.isEmpty()) {
            try {
                protocol.currentHost = addresses.get(0);
                addresses.remove(0);
                protocol.connect(protocol.currentHost.host, protocol.currentHost.port);
                if (searchType[0] && protocol.isMasterConnection()) {
                    searchType[0] = false;
                    protocol.setMustBeMasterConnection(true);
                    listener.foundActiveMaster(protocol);
                    if (!searchType[1]) return;
                    else protocol = getNewProtocol();
                } else if (searchType[1] &&  !protocol.isMasterConnection()) {
                    searchType[1] = false;
                    protocol.setMustBeMasterConnection(false);
                    listener.foundActiveSecondary(protocol);
                    if (!searchType[0]) return;
                    else {
                        listener.searchByStartName(protocol, addresses);
                        protocol = getNewProtocol();
                    }
                }
            } catch (QueryException e ) {
                log.fine("Could not connect to " + protocol.currentHost + " searching for master : " + searchType[0] + " for replica :" + searchType[1] + " error:" + e.getMessage());
            } catch (IOException e ) {
                log.fine("Could not connect to " + protocol.currentHost + " searching for master : " + searchType[0] + " for replica :" + searchType[1] + " error:" + e.getMessage());
            }
            if (!searchType[0] && !searchType[1]) return;
        }
    }

    private AuroraMultiNodesProtocol getNewProtocol() {
        AuroraMultiNodesProtocol newProtocol = new AuroraMultiNodesProtocol(this.jdbcUrl,
                this.getUsername(),
                this.getPassword(),
                this.getInfo());
        newProtocol.setProxy(proxy);
        return newProtocol;
    }


}
