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

package org.mariadb.jdbc.internal.failover.impl;

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.failover.AbstractMastersListener;
import org.mariadb.jdbc.internal.failover.HandleErrorResult;
import org.mariadb.jdbc.internal.failover.thread.FailoverLoop;
import org.mariadb.jdbc.internal.failover.tools.SearchFilter;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.util.constant.HaMode;
import org.mariadb.jdbc.internal.protocol.MasterProtocol;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.dao.ReconnectDuringTransactionException;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class MastersFailoverListener extends AbstractMastersListener {
    private final HaMode mode;

    /**
     * Initialisation.
     *
     * @param urlParser url options.
     */
    public MastersFailoverListener(final UrlParser urlParser) {
        super(urlParser);
        this.mode = urlParser.getHaMode();
        setMasterHostFail();
    }

    /**
     * Connect to database.
     *
     * @throws QueryException if connection is on error.
     */
    @Override
    public void initializeConnection() throws QueryException {
        super.initializeConnection();
        this.currentProtocol = null;
        //launching initial loop
        reconnectFailedConnection(new SearchFilter(true, false));
        resetMasterFailoverData();
    }

    /**
     * Before executing query, reconnect if connection is closed, and autoReconnect option is set.
     *
     * @throws QueryException if connection has been explicitly closed.
     */
    public void preExecute() throws QueryException {
        lastQueryNanos = System.nanoTime();
        //if connection is closed or failed on slave
        if (this.currentProtocol != null && this.currentProtocol.isClosed()) {
            preAutoReconnect();
        }
    }


    @Override
    public void preClose() throws SQLException {
        if (explicitClosed.compareAndSet(false, true)) {
            proxy.lock.lock();
            try {
                removeListenerFromSchedulers();
                closeConnection(currentProtocol);
            } finally {
                proxy.lock.unlock();
            }
        }
    }

    @Override
    public HandleErrorResult primaryFail(Method method, Object[] args) throws Throwable {
        boolean alreadyClosed = !currentProtocol.isConnected();
        boolean inTransaction = currentProtocol != null && currentProtocol.inTransaction();

        try {
            if (currentProtocol != null && currentProtocol.isConnected() && currentProtocol.ping()) {
                //connection re-established
                //if in transaction cannot be sure that the last query has been received by server of not,
                // so rollback.and throw exception
                if (currentProtocol.inTransaction()) {
                    currentProtocol.rollback();
                }
                return new HandleErrorResult(true);
            }
        } catch (QueryException e) {
            proxy.lock.lock();
            try {
                currentProtocol.close();
            } finally {
                proxy.lock.unlock();
            }
            if (setMasterHostFail()) {
                addToBlacklist(currentProtocol.getHostAddress());
            }
        }

        try {
            reconnectFailedConnection(new SearchFilter(true, false));
            handleFailLoop();
            if (alreadyClosed || (!alreadyClosed && !inTransaction && isQueryRelaunchable(method, args))) {
                return relaunchOperation(method, args);
            }
            return new HandleErrorResult(true);
        } catch (Exception e) {
            //we will throw a Connection exception that will close connection
            FailoverLoop.removeListener(this);
            return new HandleErrorResult();
        }
    }

    /**
     * Loop to connect failed hosts.
     *
     * @param searchFilter search parameters.
     * @throws QueryException if there is any error during reconnection
     */
    @Override
    public void reconnectFailedConnection(SearchFilter searchFilter) throws QueryException {
        proxy.lock.lock();
        try {
            if (!searchFilter.isInitialConnection()
                    && (isExplicitClosed() || !isMasterHostFail())) {
                return;
            }

            currentConnectionAttempts.incrementAndGet();
            resetOldsBlackListHosts();

            List<HostAddress> loopAddress = new LinkedList<>(urlParser.getHostAddresses());
            if (HaMode.FAILOVER.equals(mode)) {
                //put the list in the following order
                // - random order not connected host
                // - random order blacklist host
                // - random order connected host
                loopAddress.removeAll(getBlacklistKeys());
                Collections.shuffle(loopAddress);
                List<HostAddress> blacklistShuffle = new LinkedList<>(getBlacklistKeys());
                Collections.shuffle(blacklistShuffle);
                loopAddress.addAll(blacklistShuffle);
            } else {
                //order in sequence
                loopAddress.removeAll(getBlacklistKeys());
                loopAddress.addAll(getBlacklistKeys());
            }

            //put connected at end
            if (currentProtocol != null && !isMasterHostFail()) {
                loopAddress.remove(currentProtocol.getHostAddress());
                //loopAddress.add(currentProtocol.getHostAddress());
            }

            MasterProtocol.loop(this, loopAddress, searchFilter);
            //close loop if all connection are retrieved
            if (!isMasterHostFail()) {
                FailoverLoop.removeListener(this);
            }

            //if no error, reset failover variables
            resetMasterFailoverData();
        } finally {
            proxy.lock.unlock();
        }
    }

    /**
     * Force session to read-only according to options.
     *
     * @param mustBeReadOnly is read-only flag
     * @throws QueryException if a connection error occur
     */
    public void switchReadOnlyConnection(Boolean mustBeReadOnly) throws QueryException {
        if (urlParser.getOptions().assureReadOnly && currentReadOnlyAsked != mustBeReadOnly) {
            proxy.lock.lock();
            try {
                // verify not updated now that hold lock, double check safe due to volatile
                if (currentReadOnlyAsked != mustBeReadOnly) {
                    currentReadOnlyAsked = mustBeReadOnly;
                    setSessionReadOnly(mustBeReadOnly, currentProtocol);
                }
            } finally {
                proxy.lock.unlock();
            }
        }
    }

    /**
     * method called when a new Master connection is found after a fallback.
     *
     * @param protocol the new active connection
     */
    @Override
    public void foundActiveMaster(Protocol protocol) throws QueryException {
        if (isExplicitClosed()) {
            proxy.lock.lock();
            try {
                protocol.close();
            } finally {
                proxy.lock.unlock();
            }
            return;
        }
        syncConnection(this.currentProtocol, protocol);
        proxy.lock.lock();
        try {
            if (currentProtocol != null && !currentProtocol.isClosed()) {
                currentProtocol.close();
            }
            currentProtocol = protocol;
        } finally {
            proxy.lock.unlock();
        }

        resetMasterFailoverData();
        FailoverLoop.removeListener(this);
    }

    /**
     * Try to reconnect connection.
     *
     * @throws QueryException if reconnect a new connection but there was an active transaction.
     */
    public void reconnect() throws QueryException {
        boolean inTransaction = currentProtocol != null && currentProtocol.inTransaction();;
        reconnectFailedConnection(new SearchFilter(true, false));
        handleFailLoop();
        if (inTransaction) {
            throw new ReconnectDuringTransactionException("Connection reconnect automatically during an active transaction", 1401, "25S03");
        }
    }

    /**
     * Add listener to FailoverLoop if master connection is not active, so a reconnection will be done.
     * (the reconnection will be done by failover or if append before by the next query/method that will use
     * the failed connection)
     * Remove listener from FailoverLoop is master connection is active.
     */
    public void handleFailLoop() {
        if (isMasterHostFail()) {
            if (!isExplicitClosed()) {
                FailoverLoop.addListener(this);
            }
        } else {
            FailoverLoop.removeListener(this);
        }
    }

    public boolean isMasterConnected() {
        return currentProtocol != null && currentProtocol.isConnected();
    }

    /**
     * Check master status.
     *
     * @param searchFilter search filter
     * @return has some status changed
     */
    public boolean checkMasterStatus(SearchFilter searchFilter) {
        if (currentProtocol != null) {
            pingMasterProtocol(currentProtocol);
        }
        return false;
    }

}
