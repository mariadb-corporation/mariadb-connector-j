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
import org.mariadb.jdbc.internal.common.packet.ErrorPacket;
import org.mariadb.jdbc.internal.common.packet.PacketOutputStream;
import org.mariadb.jdbc.internal.common.packet.SyncPacketFetcher;
import org.mariadb.jdbc.internal.common.query.MySQLQuery;
import org.mariadb.jdbc.internal.common.queryresults.QueryResult;
import org.mariadb.jdbc.internal.common.queryresults.ResultSetType;
import org.mariadb.jdbc.internal.common.queryresults.SelectQueryResult;

import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MultiNodesProtocol extends MySQLProtocol {

    boolean masterConnection = false;

    public MultiNodesProtocol(JDBCUrl url,
                         final String username,
                         final String password,
                         Properties info) {
        super(url, username, password, info);
    }

    @Override
    public void connect() throws QueryException, SQLException {
        if (!isClosed()) {
            close();
        }

        // There could be several addresses given in the URL spec, try all of them, and throw exception if all hosts
        // fail.
        HostAddress[] addrs = this.jdbcUrl.getHostAddresses();
        for(int i = 0; i < addrs.length; i++) {
            currentHost = addrs[i];
            try {
                connect(currentHost.host, currentHost.port);
                return;
            } catch (IOException e) {
                if (i == addrs.length - 1) {
                    throw new QueryException("Could not connect to " + HostAddress.toString(addrs) +
                            " : " + e.getMessage(),  -1,  SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
                }
            }
        }
    }



    public void connectMaster(MultiHostListener listener) throws QueryException, SQLException  {
        //Master is considered the firstOne
        HostAddress host = jdbcUrl.getHostAddresses()[0];
        try {
            currentHost = host;
            connect(currentHost.host, currentHost.port);
            listener.foundActiveMaster(this);
        } catch (IOException e) {
            throw new QueryException("Could not connect to " + host + " : " + e.getMessage(),  -1,  SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    public void connectSecondary(MultiHostListener listener, List<HostAddress> secondaryAddresses) throws QueryException, SQLException {
        for(int i = 0; i < secondaryAddresses.size(); i++) {
            try {
                currentHost = secondaryAddresses.get(i);
                connect(currentHost.host, currentHost.port);
                listener.foundActiveSecondary(this);
                return;
            } catch (IOException e) {
                if (i == secondaryAddresses.size() - 1) {
                    throw new QueryException("Could not connect to " + HostAddress.toString(secondaryAddresses) +
                            " : " + e.getMessage(),  -1,  SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
                }
            }
        }

    }


    public boolean checkIfMaster() throws SQLException {
        masterConnection = currentHost == jdbcUrl.getHostAddresses()[0];
        return masterConnection;
    }


    public boolean isMasterConnection() {
        return masterConnection;
    }

    @Override
    public boolean createDB() {
        if (masterConnection) {
            String alias = info.getProperty("createDatabaseIfNotExist");
            return info != null
                    && (info.getProperty("createDB", "").equalsIgnoreCase("true")
                    || (alias != null && alias.equalsIgnoreCase("true")));
        }
        return false;
    }
}
