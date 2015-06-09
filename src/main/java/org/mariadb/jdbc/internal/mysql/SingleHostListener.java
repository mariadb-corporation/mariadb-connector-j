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
import org.mariadb.jdbc.internal.SQLExceptionMapper;
import org.mariadb.jdbc.internal.common.QueryException;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.SQLException;


public class SingleHostListener implements FailoverListener {

    private FailoverProxy proxy;

    public SingleHostListener() { }

    public void setProxy(FailoverProxy proxy) {
        this.proxy = proxy;
    }

    public void initializeConnection() throws QueryException, SQLException {
        this.proxy.currentProtocol.connect();
    }

    public void preExecute() throws SQLException {
        if (!proxy.currentProtocol.inTransaction() && shouldReconnect()) {
            try {
                reconnectSingleHost();
            } catch (QueryException qe) {
                SQLExceptionMapper.throwException(qe, null, null);
            }
        }
    }


    private boolean shouldReconnect() {
        return (proxy.masterHostFailTimestamp != 0 && proxy.autoReconnect && proxy.currentConnectionAttempts < proxy.maxReconnects);
    }

    protected void reconnectSingleHost() throws QueryException, SQLException {
        proxy.currentConnectionAttempts++;
        proxy.currentProtocol.connect();

        //if no error, reset failover variables
        proxy.resetMasterFailoverData();
    }


    public void switchReadOnlyConnection(Boolean readonly) {}
    public void postClose()  throws SQLException { }

    public synchronized HandleErrorResult primaryFail(Method method, Object[] args) throws Throwable {
        HandleErrorResult handleErrorResult = new HandleErrorResult();
        if (shouldReconnect()) {

            //if not first attempt to connect, wait for initialTimeout
            if (proxy.currentConnectionAttempts > 0) {
                try {
                    Thread.sleep(proxy.initialTimeout * 1000);
                } catch (InterruptedException e) { }
            }

            //trying to reconnect transparently
            reconnectSingleHost();
            if (!proxy.currentProtocol.inTransaction()) {
                handleErrorResult.resultObject = method.invoke(proxy.currentProtocol, args);
                handleErrorResult.mustThrowError = false;
            }
            return handleErrorResult;
        }
        return handleErrorResult;
    }

    public synchronized HandleErrorResult secondaryFail(Method method, Object[] args) throws Throwable {
        return new HandleErrorResult();
    }
}
