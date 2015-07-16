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
import org.mariadb.jdbc.internal.SQLExceptionMapper;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.mysql.HandleErrorResult;
import org.mariadb.jdbc.internal.mysql.Protocol;
import org.mariadb.jdbc.internal.mysql.MastersSlavesProtocol;
import org.mariadb.jdbc.internal.mysql.listener.AbstractMastersSlavesListener;
import org.mariadb.jdbc.internal.mysql.listener.tools.SearchFilter;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * this class handle the operation when multiple hosts.
 */
public class MastersSlavesListener extends AbstractMastersSlavesListener {
    private final static Logger log = Logger.getLogger(MastersSlavesListener.class.getName());

    protected Protocol masterProtocol;
    protected Protocol secondaryProtocol;
    protected long lastQueryTime = 0;
    protected ScheduledFuture scheduledPing = null;

    public MastersSlavesListener(final JDBCUrl jdbcUrl) {
        super(jdbcUrl);
        masterProtocol = null;
        secondaryProtocol = null;
        lastQueryTime = System.currentTimeMillis();

    }

    public void initializeConnection() throws QueryException {
        if (jdbcUrl.getOptions().validConnectionTimeout != 0)
            scheduledPing = executorService.scheduleWithFixedDelay(new PingLoop(this), jdbcUrl.getOptions().validConnectionTimeout, jdbcUrl.getOptions().validConnectionTimeout, TimeUnit.SECONDS);
        try {
            reconnectFailedConnection(new SearchFilter(true, true, true));
        } catch (QueryException e) {
            log.log(Level.FINEST, "initializeConnection failed", e);
            checkInitialConnection();
            throwFailoverMessage(e, false);
        }
    }

    protected void checkInitialConnection() {
        if (this.masterProtocol != null && !this.masterProtocol.isConnected()) {
            setMasterHostFail();
        }
        if (this.secondaryProtocol != null && !this.secondaryProtocol.isConnected()) {
            setSecondaryHostFail();
        }
        launchFailLoopIfNotlaunched(false);
    }

    public void preClose() throws SQLException {
        setExplicitClosed(true);
        log.finest("preClose connections");
        proxy.lock.writeLock().lock();
        try {
            if (masterProtocol != null && this.masterProtocol.isConnected()) this.masterProtocol.close();
            if (secondaryProtocol != null && this.secondaryProtocol.isConnected()) this.secondaryProtocol.close();
        } finally {
            proxy.lock.writeLock().unlock();
            if (scheduledPing != null) scheduledPing.cancel(true);

            if (scheduledFailover != null) {
                scheduledFailover.cancel(true);
                isLooping.set(false);
            }
            executorService.shutdownNow();
            try {
                executorService.awaitTermination(15, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.finest("executorService interrupted");
            }
        }
        log.finest("preClose connections end");
    }

    @Override
    public void preExecute() throws QueryException {
        //if connection is closed or failed on slave
        if (this.currentProtocol != null &&
                (this.currentProtocol.isClosed() ||

                        (!currentReadOnlyAsked.get() && !currentProtocol.isMasterConnection()))) {
            queriesSinceFailover.incrementAndGet();
            if (!isExplicitClosed() && jdbcUrl.getOptions().autoReconnect) {
                try {
                    reconnectFailedConnection(new SearchFilter(isMasterHostFail(), isSecondaryHostFail(), !currentReadOnlyAsked.get(), currentReadOnlyAsked.get()));
                } catch (QueryException e) {
                }
            } else
                throw new QueryException("Connection is closed", (short) -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState());
        }
        if (isMasterHostFail() || isSecondaryHostFail()) {
            queriesSinceFailover.incrementAndGet();
        }

        if (jdbcUrl.getOptions().validConnectionTimeout != 0) lastQueryTime = System.currentTimeMillis();
    }


    /**
     * When failing to a different type of host, when to retry
     * So he doesn't appear here.
     *
     * @return true if should reconnect.
     */
    public boolean shouldReconnect() {
        if (isMasterHostFail() || isSecondaryHostFail()) {
            if (currentConnectionAttempts.get() > jdbcUrl.getOptions().retriesAllDown) return false;
            long now = System.currentTimeMillis();

            if (isMasterHostFail()) {
                if (jdbcUrl.getOptions().queriesBeforeRetryMaster > 0 && queriesSinceFailover.get() >= jdbcUrl.getOptions().queriesBeforeRetryMaster)
                    return true;
                if (jdbcUrl.getOptions().secondsBeforeRetryMaster > 0 && (now - getMasterHostFailTimestamp()) >= jdbcUrl.getOptions().secondsBeforeRetryMaster * 1000)
                    return true;
            }

            if (isSecondaryHostFail()) {
                if (jdbcUrl.getOptions().secondsBeforeRetryMaster > 0 && (now - getSecondaryHostFailTimestamp()) >= jdbcUrl.getOptions().secondsBeforeRetryMaster * 1000)
                    return true;
            }
        }
        return false;
    }


    /**
     * Loop to connect
     *
     * @throws QueryException if there is any error during reconnection
     * @throws QueryException sqlException
     */
    public void reconnectFailedConnection(SearchFilter searchFilter) throws QueryException {
        if (log.isLoggable(Level.FINEST)) log.fine("search connection searchFilter=" + searchFilter);
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

        if (secondaryProtocol != null && !isSecondaryHostFail()) {
            loopAddress.remove(secondaryProtocol.getHostAddress());
            //loopAddress.add(secondaryProtocol.getHostAddress());
        }

        if (((searchFilter.isSearchForMaster() && isMasterHostFail()) || (searchFilter.isSearchForSlave() && isSecondaryHostFail())) || searchFilter.isInitialConnection()) {
            MastersSlavesProtocol.loop(this, loopAddress, blacklist, searchFilter);
        }
    }

    /**
     * method called when a new Master connection is found after a fallback
     *
     * @param newMasterProtocol the new active connection
     */
    public void foundActiveMaster(Protocol newMasterProtocol) {
        if (isExplicitClosed()) {
            newMasterProtocol.close();
            return;
        }
        log.finest("log $$1 " + proxy.lock.getReadLockCount() + " " + proxy.lock.getWriteHoldCount());

        proxy.lock.writeLock().lock();
        try {
            log.finest("log $$2 " + proxy.lock.getReadLockCount()+ " " + proxy.lock.getWriteHoldCount());
            if (masterProtocol != null && !masterProtocol.isClosed()) masterProtocol.close();
            this.masterProtocol = (MastersSlavesProtocol) newMasterProtocol;
            if (!currentReadOnlyAsked.get() || isSecondaryHostFail()) {
                //actually on a secondary read-only because master was unknown.
                //So select master as currentConnection
                try {
                    syncConnection(currentProtocol, this.masterProtocol);
                } catch (Exception e) {
                    log.log(Level.FINE, "Some error append during connection parameter synchronisation : ", e);
                }
                log.finest("switching current connection to master connection");
                currentProtocol = this.masterProtocol;
            }

            if (log.isLoggable(Level.FINE)) {
                if (getMasterHostFailTimestamp() > 0) {
                    log.fine("new primary node [" + newMasterProtocol.getHostAddress().toString() + "] connection established after " + (System.currentTimeMillis() - getMasterHostFailTimestamp()));
                } else
                    log.fine("new primary node [" + newMasterProtocol.getHostAddress().toString() + "] connection established");
            }
            resetMasterFailoverData();
            if (!isSecondaryHostFail()) stopFailover();
        } finally {
            proxy.lock.writeLock().unlock();
            log.finest("log $$3 " + proxy.lock.getReadLockCount()+ " " + proxy.lock.getWriteHoldCount());

        }

    }


    /**
     * method called when a new secondary connection is found after a fallback
     *
     * @param newSecondaryProtocol the new active connection
     */
    public void foundActiveSecondary(Protocol newSecondaryProtocol) {
        if (isExplicitClosed()) {
            newSecondaryProtocol.close();
            return;
        }

        proxy.lock.writeLock().lock();
        try {
            if (secondaryProtocol != null && !secondaryProtocol.isClosed()) secondaryProtocol.close();

            log.finest("found active secondary connection");
            this.secondaryProtocol = newSecondaryProtocol;

            //if asked to be on read only connection, switching to this new connection
            if (currentReadOnlyAsked.get() || (jdbcUrl.getOptions().failOnReadOnly && !currentReadOnlyAsked.get() && isMasterHostFail())) {
                try {
                    syncConnection(currentProtocol, this.secondaryProtocol);
                } catch (Exception e) {
                    log.log(Level.FINE, "Some error append during connection parameter synchronisation : ", e);
                }
                currentProtocol = this.secondaryProtocol;
            }

            if (log.isLoggable(Level.FINE)) {
                if (getSecondaryHostFailTimestamp() > 0) {
                    log.fine("new active secondary node [" + newSecondaryProtocol.getHostAddress().toString() + "] connection established after " + (System.currentTimeMillis() - getSecondaryHostFailTimestamp()));
                } else
                    log.fine("new active secondary node [" + newSecondaryProtocol.getHostAddress().toString() + "] connection established");

            }
            resetSecondaryFailoverData();
            if (!isMasterHostFail()) stopFailover();
        } finally {
            proxy.lock.writeLock().unlock();
        }
    }

    /**
     * switch to a read-only(secondary) or read and write connection(master)
     *
     * @param mustBeReadOnly the read-only status asked
     * @throws QueryException if operation hasn't change protocol
     */
    @Override
    public void switchReadOnlyConnection(Boolean mustBeReadOnly) throws QueryException {
        if (log.isLoggable(Level.FINEST)) log.fine("switching to mustBeReadOnly = " + mustBeReadOnly + " mode");

        if (mustBeReadOnly != currentReadOnlyAsked.get() && currentProtocol.inTransaction()) {
            throw new QueryException("Trying to set to read-only mode during a transaction");
        }
        if (currentReadOnlyAsked.compareAndSet(!mustBeReadOnly, mustBeReadOnly)) {
            if (currentReadOnlyAsked.get()) {
                if (currentProtocol.isMasterConnection()) {
                    //must change to replica connection
                    if (!isSecondaryHostFail()) {
                        proxy.lock.writeLock().lock();
                        try {

                            log.finest("switching to secondary connection");
                            syncConnection(this.masterProtocol, this.secondaryProtocol);

                            currentProtocol = this.secondaryProtocol;
                            setSessionReadOnly(true);

                            log.finest("current connection is now secondary");
                            return;
                        } catch (QueryException e) {
                            log.log(Level.FINEST, "switching to secondary connection failed", e);
                            if (setSecondaryHostFail()) {
                                addToBlacklist(secondaryProtocol.getHostAddress());
                            }
                        } finally {
                            proxy.lock.writeLock().unlock();
                        }
                    }
                    launchFailLoopIfNotlaunched(false);
                    throwFailoverMessage(new QueryException("master " + masterProtocol.getHostAddress() + " connection failed"), false);
                }
            } else {
                if (!currentProtocol.isMasterConnection()) {
                    //must change to master connection
                    if (!isMasterHostFail()) {

                        proxy.lock.writeLock().lock();
                        try {
                            log.finest("switching to master connection");

                            syncConnection(this.secondaryProtocol, this.masterProtocol);
                            currentProtocol = this.masterProtocol;

                            log.fine("current connection is now master");
                            return;
                        } catch (QueryException e) {
                            log.log(Level.FINE, "switching to master connection failed", e);
                            if (setMasterHostFail()) {
                                addToBlacklist(masterProtocol.getHostAddress());
                            }
                        } finally {
                            proxy.lock.writeLock().unlock();
                        }
                    }
                    if (jdbcUrl.getOptions().autoReconnect) {
                        reconnectFailedConnection(new SearchFilter(false, true, false, true));
                        //connection established, no need to send Exception !
                        log.finest("switching to master connection");
                        proxy.lock.writeLock().lock();
                        try {
                            syncConnection(this.secondaryProtocol, this.masterProtocol);
                            currentProtocol = this.masterProtocol;
                        } finally {
                            proxy.lock.writeLock().unlock();
                        }
                        log.fine("current connection is now master");
                        return;
                    }
                    launchFailLoopIfNotlaunched(false);
                    throwFailoverMessage(new QueryException("master " + masterProtocol.getHostAddress() + " connection failed"), false);
                }
            }
        }
    }

    /**
     * to handle the newly detected failover on the master connection
     *
     * @param method the initial called method
     * @param args   the initial args
     * @return an object to indicate if the previous Exception must be thrown, or the object resulting if a failover worked
     * @throws Throwable if failover has not been catch
     */
    public HandleErrorResult primaryFail(Method method, Object[] args) throws Throwable {
        boolean alreadyClosed = !masterProtocol.isConnected();

        //try to reconnect automatically only time before looping
        try {
            if (masterProtocol != null && masterProtocol.isConnected() && masterProtocol.ping()) {
                if (log.isLoggable(Level.FINE))
                    log.fine("Primary node [" + masterProtocol.getHostAddress().toString() + "] connection re-established");

                // if in transaction cannot be sure that the last query has been received by server of not, so rollback.
                if (masterProtocol.inTransaction()) {
                    masterProtocol.rollback();
                }
                return new HandleErrorResult(true);
            }
        } catch (QueryException e) {
            proxy.lock.writeLock().lock();
            try {
                masterProtocol.close();
            } finally {
                proxy.lock.writeLock().unlock();
            }

            if (setMasterHostFail()) addToBlacklist(masterProtocol.getHostAddress());
        }

        //fail on slave if parameter permit so
        if (jdbcUrl.getOptions().failOnReadOnly) {
            //in multiHost, switch to secondary if active, even if in a current transaction -> will throw an exception
            if (!isSecondaryHostFail()) {
                try {
                    if (this.secondaryProtocol != null && this.secondaryProtocol.ping()) {
                        log.finest("switching to secondary connection");
                        syncConnection(masterProtocol, this.secondaryProtocol);
                        proxy.lock.writeLock().lock();
                        try {
                            currentProtocol = this.secondaryProtocol;
                        } finally {
                            proxy.lock.writeLock().unlock();
                        }
                        launchFailLoopIfNotlaunched(false);
                        try {
                            return relaunchOperation(method, args);
                        } catch (Exception e) {
                            log.log(Level.FINEST, "relaunchOperation failed", e);
                        }
                        return new HandleErrorResult();
                    } else log.finest("ping failed on secondary");
                } catch (Exception e) {
                    if (setSecondaryHostFail()) addToBlacklist(this.secondaryProtocol.getHostAddress());
                    if (secondaryProtocol.isConnected()) {
                        proxy.lock.writeLock().lock();
                        try {
                            secondaryProtocol.close();
                        } finally {
                            proxy.lock.writeLock().unlock();
                        }
                    }
                    log.log(Level.FINEST, "ping on secondary failed");
                }
            }
        }

        try {
            reconnectFailedConnection(new SearchFilter(true, jdbcUrl.getOptions().failOnReadOnly, true, jdbcUrl.getOptions().failOnReadOnly));
            if (isMasterHostFail()) launchFailLoopIfNotlaunched(true);
            if (alreadyClosed) return relaunchOperation(method, args);
            return new HandleErrorResult(true);
        } catch (Exception e) {
            launchFailLoopIfNotlaunched(true);
            return new HandleErrorResult();
        }
    }


    public void reconnect() throws QueryException {
        SearchFilter filter;
        if (currentReadOnlyAsked.get()) {
            filter = new SearchFilter(true, true, true, true);
        } else {
            filter = new SearchFilter(true, jdbcUrl.getOptions().failOnReadOnly, true, jdbcUrl.getOptions().failOnReadOnly);
        }
        reconnectFailedConnection(filter);
    }


    /**
     * to handle the newly detected failover on the secondary connection
     *
     * @param method the initial called method
     * @param args   the initial args
     * @return an object to indicate if the previous Exception must be thrown, or the object resulting if a failover worked
     * @throws Throwable if failover has not catch error
     */
    public HandleErrorResult secondaryFail(Method method, Object[] args) throws Throwable {
        try {
            if (this.secondaryProtocol != null && secondaryProtocol.isConnected() && this.secondaryProtocol.ping()) {
                if (log.isLoggable(Level.FINE))
                    log.fine("Secondary node [" + this.secondaryProtocol.getHostAddress().toString() + "] connection re-established");
                return relaunchOperation(method, args);
            }
        } catch (Exception e) {
            log.finest("ping fail on secondary");
            proxy.lock.writeLock().lock();
            try {
                secondaryProtocol.close();
            } finally {
                proxy.lock.writeLock().unlock();
            }

            if (setSecondaryHostFail()) addToBlacklist(this.secondaryProtocol.getHostAddress());
        }

        if (!isMasterHostFail()) {
            try {
                if (masterProtocol != null) {
                    this.masterProtocol.ping(); //check that master is on before switching to him
                    log.finest("switching to master connection");
                    syncConnection(secondaryProtocol, masterProtocol);
                    proxy.lock.writeLock().lock();
                    try {
                        currentProtocol = masterProtocol;
                    } finally {
                        proxy.lock.writeLock().unlock();
                    }
                    launchFailLoopIfNotlaunched(true); //launch reconnection loop
                    return relaunchOperation(method, args); //now that we are on master, relaunched result if the result was not crashing the master
                }
            } catch (Exception e) {
                log.finest("ping fail on master");
                if (setMasterHostFail()) {
                    addToBlacklist(masterProtocol.getHostAddress());
                    if (masterProtocol.isConnected()) {
                        proxy.lock.writeLock().lock();
                        try {
                            masterProtocol.close();
                        } finally {
                            proxy.lock.writeLock().unlock();
                        }
                    }
                }
            }
        }

        try {
            reconnectFailedConnection(new SearchFilter(true, true, true, true));
            if (!isSecondaryHostFail()) {
                if (log.isLoggable(Level.FINE))
                    log.fine("SQL Secondary node [" + this.masterProtocol.getHostAddress().toString() + "] connection re-established");
            } else {
                log.finest("switching to master connection");
                syncConnection(this.secondaryProtocol, this.masterProtocol);
                proxy.lock.writeLock().lock();
                try {
                    currentProtocol = this.masterProtocol;
                } finally {
                    proxy.lock.writeLock().unlock();
                }
            }
            return relaunchOperation(method, args); //now that we are reconnect, relaunched result if the result was not crashing the node
        } catch (Exception ee) {
            launchFailLoopIfNotlaunched(false);
            return new HandleErrorResult();
        }
    }


    /**
     * private class to chech of currents connections are still ok.
     */
    protected class PingLoop implements Runnable {
        MastersSlavesListener listener;

        public PingLoop(MastersSlavesListener listener) {
            this.listener = listener;
        }

        public void run() {
            if (lastQueryTime + jdbcUrl.getOptions().validConnectionTimeout * 1000 < System.currentTimeMillis()) {
                log.finest("PingLoop run ");
                if (!isMasterHostFail()) {
                    log.finest("PingLoop run, master not seen failed");
                    boolean masterFail = false;
                    try {

                        if (masterProtocol != null && masterProtocol.isConnected()) {
                            checkIfTypeHaveChanged(null);
                        } else {
                            masterFail = true;
                        }
                    } catch (QueryException e) {
                        log.log(Level.FINEST, "PingLoop ping to master error", e);
                        masterFail = true;
                    }

                    if (masterFail) {
                        log.finest("PingLoop master failed -> will loop to found it");
                        if (setMasterHostFail()) {
                            try {
                                listener.primaryFail(null, null);
                            } catch (Throwable t) {
                                //do nothing
                            }
                        }
                    }
                }
            }
        }
    }

    public void checkIfTypeHaveChanged(SearchFilter searchFilter) throws QueryException {
        if (masterProtocol.ping()) {
            log.finest("PingLoop master ping ok");
        }
    }


    /**
     * Throw a human readable message after a failoverException
     *
     * @param queryException internal error
     * @param reconnected    connection status
     * @throws QueryException error with failover information
     */
    @Override
    public void throwFailoverMessage(QueryException queryException, boolean reconnected) throws QueryException {
        boolean connectionTypeMaster = true;
        HostAddress hostAddress = (masterProtocol != null) ? masterProtocol.getHostAddress() : null;
        if (currentReadOnlyAsked.get()) {
            connectionTypeMaster = false;
            hostAddress = (secondaryProtocol != null) ? secondaryProtocol.getHostAddress() : null;
        }

        String firstPart = "Communications link failure with " + (connectionTypeMaster ? "primary" : "secondary") + ((hostAddress != null) ? " host " + hostAddress.host + ":" + hostAddress.port : "") + ". ";
        String error = "";
        if (jdbcUrl.getOptions().autoReconnect || (!isMasterHostFail() && !isSecondaryHostFail())) {
            if ((connectionTypeMaster && isMasterHostFail()) || (!connectionTypeMaster && isSecondaryHostFail()))
                error += "  Driver will reconnect automatically in a few millisecond or during next query if append before";
            else error += " Driver as successfully reconnect connection";
        } else {
            if (reconnected) {
                error += " Driver as reconnect connection";
            } else {
                if (currentConnectionAttempts.get() > jdbcUrl.getOptions().retriesAllDown) {
                    error += " Driver will not try to reconnect (too much failure > " + jdbcUrl.getOptions().retriesAllDown + ")";
                } else {
                    if (shouldReconnect()) {
                        error += " Driver will try to reconnect automatically in a few millisecond or during next query if append before";
                    } else {
                        long longestFail = isMasterHostFail() ? (isSecondaryHostFail() ? Math.min(getMasterHostFailTimestamp(), getSecondaryHostFailTimestamp()) : getMasterHostFailTimestamp()) : getSecondaryHostFailTimestamp();
                        long nextReconnectionTime = jdbcUrl.getOptions().secondsBeforeRetryMaster * 1000 - (System.currentTimeMillis() - longestFail);
                        if (jdbcUrl.getOptions().secondsBeforeRetryMaster > 0) {
                            if (jdbcUrl.getOptions().queriesBeforeRetryMaster > 0) {
                                error += " Driver will try to reconnect " + (connectionTypeMaster ? "primary" : "secondary") + " after " + nextReconnectionTime + " milliseconds or after " + (jdbcUrl.getOptions().queriesBeforeRetryMaster - queriesSinceFailover.get()) + " query(s)";
                            } else {
                                error += " Driver will try to reconnect " + (connectionTypeMaster ? "primary" : "secondary") + " after " + nextReconnectionTime + " milliseconds";
                            }
                        } else {
                            if (jdbcUrl.getOptions().queriesBeforeRetryMaster > 0) {
                                error += " Driver will try to reconnect " + (connectionTypeMaster ? "primary" : "secondary") + " after " + (jdbcUrl.getOptions().queriesBeforeRetryMaster - queriesSinceFailover.get()) + " query(s)";
                            } else {
                                error += " Driver will not try to reconnect automatically";
                            }
                        }
                    }

                }
            }
        }
        if (queryException == null) {
            throw new QueryException(firstPart + error, (short) -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState());
        } else {
            error = queryException.getMessage() + ". " + error;
            queryException.setMessage(firstPart + error);
            throw queryException;
        }
    }
}
