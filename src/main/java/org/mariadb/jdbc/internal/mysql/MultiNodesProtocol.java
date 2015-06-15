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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

public class MultiNodesProtocol extends MySQLProtocol {

    boolean masterConnection = false;
    boolean mustBeMasterConnection = false;

    public MultiNodesProtocol(JDBCUrl url) {
        super(url);
    }

    @Override
    public void connect() throws QueryException {
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



    public void connectMaster(MultiHostListener listener) throws QueryException  {
        //Master is considered the firstOne
        HostAddress host = jdbcUrl.getHostAddresses()[0];
        try {
            currentHost = host;
            log.fine("trying to connect master " + currentHost);
            connect(currentHost.host, currentHost.port);
            listener.foundActiveMaster(this);
            setMustBeMasterConnection(true);
        } catch (IOException e) {
            throw new QueryException("Could not connect to " + host + " : " + e.getMessage(), -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    public void connectSecondary(MultiHostListener listener) throws QueryException {
        //first Host is master, so not taken
        for(int i = 1; i < jdbcUrl.getHostAddresses().length; i++) {
            try {
                currentHost = jdbcUrl.getHostAddresses()[i];
                log.fine("trying to connect slave "+currentHost);
                connect(currentHost.host, currentHost.port);
                listener.foundActiveSecondary(this);
                setMustBeMasterConnection(false);
                return;
            } catch (IOException e) {
                if (i == jdbcUrl.getHostAddresses().length - 1) {
                    throw new QueryException("Could not connect to " + HostAddress.toString(jdbcUrl.getHostAddresses()) +
                            " : " + e.getMessage(),  -1,  SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
                }
            }
        }
    }


    public boolean checkIfMaster() throws QueryException {
        masterConnection = currentHost == jdbcUrl.getHostAddresses()[0];
        return masterConnection;
    }


    public boolean isMasterConnection() {
        return masterConnection;
    }

    @Override
    public boolean createDB() {
        if (masterConnection) {
            String alias = jdbcUrl.getProperties().getProperty("createDatabaseIfNotExist");
            return jdbcUrl.getProperties() != null
                    && (jdbcUrl.getProperties().getProperty("createDB", "").equalsIgnoreCase("true")
                    || (alias != null && alias.equalsIgnoreCase("true")));
        }
        return false;
    }

    public boolean mustBeMasterConnection() {
        return mustBeMasterConnection;
    }
    public void setMustBeMasterConnection(boolean mustBeMasterConnection) {
        this.mustBeMasterConnection = mustBeMasterConnection;
    }
}
