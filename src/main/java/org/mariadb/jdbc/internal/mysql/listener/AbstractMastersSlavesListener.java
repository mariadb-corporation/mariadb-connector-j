package org.mariadb.jdbc.internal.mysql.listener;

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

import org.mariadb.jdbc.JDBCUrl;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.mysql.FailoverProxy;
import org.mariadb.jdbc.internal.mysql.HandleErrorResult;
import org.mariadb.jdbc.internal.mysql.Protocol;
import org.mariadb.jdbc.internal.mysql.listener.tools.SearchFilter;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public abstract class AbstractMastersSlavesListener extends AbstractMastersListener {

    protected AtomicInteger queriesSinceFailover = new AtomicInteger();
    /* =========================== Failover variables ========================================= */
    private AtomicLong secondaryHostFailTimestamp = new AtomicLong();
    private AtomicBoolean secondaryHostFail = new AtomicBoolean();

    protected AbstractMastersSlavesListener(JDBCUrl jdbcUrl) {
        super(jdbcUrl);
        this.secondaryHostFail.set(true);
    }

    public HandleErrorResult handleFailover(Method method, Object[] args) throws Throwable {
        if (explicitClosed) throw new QueryException("Connection has been closed !");
        if (currentProtocol.mustBeMasterConnection()) {
            if (setMasterHostFail()) {
//                log.warn("SQL Primary node [" + this.currentProtocol.getHostAddress().toString() + "] connection fail ");
                addToBlacklist(currentProtocol.getHostAddress());
                if (FailoverProxy.METHOD_EXECUTE_QUERY.equals(method.getName())) queriesSinceFailover.incrementAndGet();
            }
            return primaryFail(method, args);
        } else {
            if (setSecondaryHostFail()) {
//                log.warn("SQL Secondary node [" + this.currentProtocol.getHostAddress().toString() + "] connection fail ");
                addToBlacklist(currentProtocol.getHostAddress());
                if (FailoverProxy.METHOD_EXECUTE_QUERY.equals(method.getName())) queriesSinceFailover.incrementAndGet();
            }
            return secondaryFail(method, args);
        }
    }

    @Override
    protected void resetMasterFailoverData() {
        super.resetMasterFailoverData();

        //if all connection are up, reset failovers timers
        if (!secondaryHostFail.get()) {
            currentConnectionAttempts.set(0);
            lastRetry = 0;
            queriesSinceFailover.set(0);
            ;
        }
    }

    protected void resetSecondaryFailoverData() {
        if (secondaryHostFail.compareAndSet(true, false)) secondaryHostFailTimestamp.set(0);

        //if all connection are up, reset failovers timers
        if (!isMasterHostFail()) {
            currentConnectionAttempts.set(0);
            lastRetry = 0;
            queriesSinceFailover.set(0);
        }
    }

    public long getSecondaryHostFailTimestamp() {
        return secondaryHostFailTimestamp.get();
    }

    public boolean setSecondaryHostFail() {
        if (secondaryHostFail.compareAndSet(false, true)) {
            secondaryHostFailTimestamp.set(System.currentTimeMillis());
            currentConnectionAttempts.set(0);
            return true;
        }
        return false;
    }

    public boolean isSecondaryHostFail() {
        return secondaryHostFail.get();
    }

    public boolean hasHostFail() {
        return isMasterHostFail() || isSecondaryHostFail();
    }

    public SearchFilter getFilterForFailedHost() {
        return new SearchFilter(isMasterHostFail(), isSecondaryHostFail());
    }

    public abstract HandleErrorResult secondaryFail(Method method, Object[] args) throws Throwable;

    public abstract void foundActiveSecondary(Protocol newSecondaryProtocol) throws QueryException;

}
