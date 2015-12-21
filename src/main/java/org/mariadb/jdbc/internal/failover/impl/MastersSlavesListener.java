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
import org.mariadb.jdbc.internal.failover.AbstractMastersSlavesListener;
import org.mariadb.jdbc.internal.util.ExceptionMapper;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.failover.HandleErrorResult;
import org.mariadb.jdbc.internal.protocol.MastersSlavesProtocol;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.failover.tools.SearchFilter;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


/**
 * this class handle the operation when multiple hosts.
 */
public class MastersSlavesListener extends AbstractMastersSlavesListener {

    protected Protocol masterProtocol;
    protected Protocol secondaryProtocol;
    protected long lastQueryNanos = 0;
    protected final PingLoop pingLoop;

    /**
     * Initialisation.
     * @param urlParser connection string object.
     */
    public MastersSlavesListener(final UrlParser urlParser) {
        super(urlParser);
        masterProtocol = null;
        secondaryProtocol = null;
        lastQueryNanos = System.nanoTime();
        pingLoop = new PingLoop(this);
    }

    /**
     * Initialize connections.
     * @throws QueryException if a connection error append.
     */
    public void initializeConnection() throws QueryException {
        if (urlParser.getOptions().validConnectionTimeout != 0) {
            long scheduleMillis = TimeUnit.SECONDS.toMillis(urlParser.getOptions().validConnectionTimeout);
            pingLoop.scheduleSelf(scheduler, scheduleMillis);
        }
        try {
            reconnectFailedConnection(new SearchFilter(true, true, true));
        } catch (QueryException e) {
            //initializeConnection failed
            checkInitialConnection(e);
        }
    }

    protected void checkInitialConnection(QueryException queryException) throws QueryException {
        boolean masterFail = false;
        if (this.masterProtocol != null && !this.masterProtocol.isConnected()) {
            masterFail = true;
        }
        if (this.secondaryProtocol != null && !this.secondaryProtocol.isConnected()) {
            setSecondaryHostFail();
            if (!masterFail) {
                //launched failLoop only if not throwing connection (connection will be closed).
                handleFailLoop(false);
            }
        }
        if (masterFail) {
            setMasterHostFail();
            throwFailoverMessage(masterProtocol.getHostAddress(), true, queryException, false);
        }
    }

    /**
     * Called after a call on Connection.close(). Will explicitly closed all connections.
     * @throws SQLException if error append during closing those connections.
     */
    public void preClose() throws SQLException {
        if (!isExplicitClosed()) {
            proxy.lock.lock();
            try {
                setExplicitClosed(true);

                //closing first additional thread if running to avoid connection creation before closing
                pingLoop.blockTillTerminated();
                shutdownScheduler();

                //closing connections
                if (masterProtocol != null && this.masterProtocol.isConnected()) {
                    this.masterProtocol.close();
                }
                if (secondaryProtocol != null && this.secondaryProtocol.isConnected()) {
                    this.secondaryProtocol.close();
                }
            } finally {
                proxy.lock.unlock();
            }
        }
    }

    @Override
    public void preExecute() throws QueryException {
        //if connection is closed or failed on slave
        if (this.currentProtocol != null
                && (this.currentProtocol.isClosed() || (!currentReadOnlyAsked.get() && !currentProtocol.isMasterConnection()))) {
            if (!isExplicitClosed() && urlParser.getOptions().autoReconnect) {
                try {
                    reconnectFailedConnection(new SearchFilter(isMasterHostFail(), isSecondaryHostFail(),
                            !currentReadOnlyAsked.get(), currentReadOnlyAsked.get()));
                } catch (QueryException e) {
                    //eat exception
                }
                handleFailLoop(true);
            } else {
                throw new QueryException("Connection is closed", (short) -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState());
            }
        }

        if (urlParser.getOptions().validConnectionTimeout != 0) {
            lastQueryNanos = System.nanoTime();
        }
    }

    /**
     * Loop to connect.
     *
     * @throws QueryException if there is any error during reconnection
     * @throws QueryException sqlException
     */
    public void reconnectFailedConnection(SearchFilter searchFilter) throws QueryException {
//        if (log.isTraceEnabled()) log.trace("search connection searchFilter=" + searchFilter);
        currentConnectionAttempts.incrementAndGet();
        resetOldsBlackListHosts();

        //put the list in the following order
        // - random order not connected host
        // - random order blacklist host
        // - random order connected host
        List<HostAddress> loopAddress = new LinkedList<>(urlParser.getHostAddresses());
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

        if (((searchFilter.isSearchForMaster() && isMasterHostFail()) || (searchFilter.isSearchForSlave() && isSecondaryHostFail()))
                || searchFilter.isInitialConnection()) {
            MastersSlavesProtocol.loop(this, loopAddress, blacklist, searchFilter);
        }

        //close loop if all connection are retrieved
        if (!isMasterHostFail() && !isSecondaryHostFail()) {
            stopFailover();
        }

    }

    /**
     * Method called when a new Master connection is found after a fallback.
     *
     * @param newMasterProtocol the new active connection
     */
    public void foundActiveMaster(Protocol newMasterProtocol) {
        if (isMasterHostFail()) {
            if (isExplicitClosed()) {
                newMasterProtocol.close();
                return;
            }

            proxy.lock.lock();
            try {

                if (masterProtocol != null && !masterProtocol.isClosed()) {
                    masterProtocol.close();
                }

                if (!currentReadOnlyAsked.get() || isSecondaryHostFail()) {
                    //actually on a secondary read-only because master was unknown.
                    //So select master as currentConnection
                    try {
                        syncConnection(currentProtocol, newMasterProtocol);
                    } catch (Exception e) {
                        //Some error append during connection parameter synchronisation
                    }
                    //switching current connection to master connection
                    currentProtocol = newMasterProtocol;
                }

                this.masterProtocol = newMasterProtocol;
                resetMasterFailoverData();
            } finally {
                proxy.lock.unlock();
            }
        } else {
            newMasterProtocol.close();
        }

    }


    /**
     * Method called when a new secondary connection is found after a fallback.
     *
     * @param newSecondaryProtocol the new active connection
     */
    public void foundActiveSecondary(Protocol newSecondaryProtocol) throws QueryException {
        if (isSecondaryHostFail()) {
            if (isExplicitClosed()) {
                newSecondaryProtocol.close();
                return;
            }
            proxy.lock.lock();
            try {

                if (secondaryProtocol != null && !secondaryProtocol.isClosed()) {
                    System.out.println(Thread.currentThread().getName() + " foundActiveSecondary -> secondaryProtocol.close");
                    secondaryProtocol.close();
                }

                //if asked to be on read only connection, switching to this new connection
                if (currentReadOnlyAsked.get() || (urlParser.getOptions().failOnReadOnly && !currentReadOnlyAsked.get() && isMasterHostFail())) {
                    try {
                        syncConnection(currentProtocol, newSecondaryProtocol);
                    } catch (Exception e) {
                        //Some error append during connection parameter synchronisation
                    }
                    currentProtocol = newSecondaryProtocol;
                }

                //set new found connection as slave connection.
                this.secondaryProtocol = newSecondaryProtocol;
                if (urlParser.getOptions().assureReadOnly) {
                    setSessionReadOnly(true, this.secondaryProtocol);
                }

                resetSecondaryFailoverData();
            } finally {
                proxy.lock.unlock();
            }
        } else {
            newSecondaryProtocol.close();
        }
    }

    /**
     * Switch to a read-only(secondary) or read and write connection(master).
     *
     * @param mustBeReadOnly the read-only status asked
     * @throws QueryException if operation hasn't change protocol
     */
    @Override
    public void switchReadOnlyConnection(Boolean mustBeReadOnly) throws QueryException {
        if (mustBeReadOnly != currentReadOnlyAsked.get() && currentProtocol.inTransaction()) {
            throw new QueryException("Trying to set to read-only mode during a transaction");
        }
        if (currentReadOnlyAsked.compareAndSet(!mustBeReadOnly, mustBeReadOnly)) {
            if (currentReadOnlyAsked.get()) {
                if (currentProtocol.isMasterConnection()) {
                    //must change to replica connection
                    if (!isSecondaryHostFail()) {
                        proxy.lock.lock();
                        try {
                            //switching to secondary connection
                            syncConnection(this.masterProtocol, this.secondaryProtocol);
                            currentProtocol = this.secondaryProtocol;
                            //current connection is now secondary
                            return;
                        } catch (QueryException e) {
                            //switching to secondary connection failed
                            if (setSecondaryHostFail()) {
                                addToBlacklist(secondaryProtocol.getHostAddress());
                            }
                        } finally {
                            proxy.lock.unlock();
                        }
                    }
                    //stay on master connection, since slave connection is fail
                    launchFailLoopIfNotLaunched(false);
                }
            } else {
                if (!currentProtocol.isMasterConnection()) {
                    //must change to master connection
                    if (!isMasterHostFail()) {
                        proxy.lock.lock();
                        try {
                            //switching to master connection
                            syncConnection(this.secondaryProtocol, this.masterProtocol);
                            currentProtocol = this.masterProtocol;
                            //current connection is now master
                            return;
                        } catch (QueryException e) {
                            //switching to master connection failed
                            if (setMasterHostFail()) {
                                addToBlacklist(masterProtocol.getHostAddress());
                            }
                        } finally {
                            proxy.lock.unlock();
                        }
                    }

                    try {
                        reconnectFailedConnection(new SearchFilter(true, false, true, false));
                        handleFailLoop(false);
                        //connection established, no need to send Exception !
                        //switching to master connection
                        proxy.lock.lock();
                        try {
                            syncConnection(this.secondaryProtocol, this.masterProtocol);
                            currentProtocol = this.masterProtocol;
                        } catch (QueryException e) {
                            //switching to master connection failed
                            if (setMasterHostFail()) {
                                addToBlacklist(masterProtocol.getHostAddress());
                            }
                        } finally {
                            proxy.lock.unlock();
                        }
                        //current connection is now master
                        return;
                    } catch (QueryException e) {
                        //stop failover, since we will throw a connection exception that will close the connection.
                        stopFailover();
                        HostAddress failHost = (this.masterProtocol != null ) ? this.masterProtocol.getHostAddress() : null;
                        throwFailoverMessage(failHost, true, new QueryException("master "
                                + masterProtocol.getHostAddress() + " connection failed"), false);
                    }

                }
            }
        }
    }

    /**
     * To handle the newly detected failover on the master connection.
     *
     * @param method the initial called method
     * @param args   the initial args
     * @return an object to indicate if the previous Exception must be thrown, or the object resulting if a failover worked
     * @throws Throwable if failover has not been catch
     */
    public HandleErrorResult primaryFail(Method method, Object[] args) throws Throwable {
        boolean alreadyClosed = !masterProtocol.isConnected();

        //try to reconnect automatically only time before looping
        if (tryPingOnMaster()) {
            return new HandleErrorResult(true);
        }

        //fail on slave if parameter permit so
        if (urlParser.getOptions().failOnReadOnly && !isSecondaryHostFail()) {
            try {
                if (this.secondaryProtocol != null && this.secondaryProtocol.ping()) {
                    //switching to secondary connection
                    syncConnection(masterProtocol, this.secondaryProtocol);
                    proxy.lock.lock();
                    try {
                        currentProtocol = this.secondaryProtocol;
                    } finally {
                        proxy.lock.unlock();
                    }
                    launchFailLoopIfNotLaunched(false);
                    try {
                        return relaunchOperation(method, args);
                    } catch (Exception e) {
                        //relaunchOperation failed
                    }
                    return new HandleErrorResult();
                }
            } catch (Exception e) {
                if (setSecondaryHostFail()) {
                    addToBlacklist(this.secondaryProtocol.getHostAddress());
                }
                if (secondaryProtocol.isConnected()) {
                    proxy.lock.lock();
                    try {
                        secondaryProtocol.close();
                    } finally {
                        proxy.lock.unlock();
                    }
                }
            }
        }

        try {
            reconnectFailedConnection(new SearchFilter(true, urlParser.getOptions().failOnReadOnly, true, urlParser.getOptions().failOnReadOnly));
            handleFailLoop(true);
            if (alreadyClosed || currentReadOnlyAsked.get()) {
                return relaunchOperation(method, args);
            }
            return new HandleErrorResult(true);
        } catch (Exception e) {
            //we will throw a Connection exception that will close connection
            stopFailover();
            return new HandleErrorResult();
        }
    }

    /**
     * Reconnect failed connection.
     * @throws QueryException if reconnection has failed
     */
    public void reconnect() throws QueryException {
        SearchFilter filter;
        if (currentReadOnlyAsked.get()) {
            filter = new SearchFilter(true, true, true, true);
        } else {
            filter = new SearchFilter(true, urlParser.getOptions().failOnReadOnly, true, urlParser.getOptions().failOnReadOnly);
        }
        reconnectFailedConnection(filter);
        handleFailLoop(false);
    }

    private boolean tryPingOnMaster() {
        try {
            if (masterProtocol != null && masterProtocol.isConnected() && masterProtocol.ping()) {
                if (masterProtocol.inTransaction()) {
                    masterProtocol.rollback();
                }
                return true;
            }
        } catch (QueryException e) {
            proxy.lock.lock();
            try {
                masterProtocol.close();
            } finally {
                proxy.lock.unlock();
            }

            if (setMasterHostFail()) {
                addToBlacklist(masterProtocol.getHostAddress());
            }
        }
        return false;
    }

    private boolean tryPingOnSecondary() {
        try {
            if (this.secondaryProtocol != null && secondaryProtocol.isConnected() && this.secondaryProtocol.ping()) {
                return true;
            }
        } catch (Exception e) {
            proxy.lock.lock();
            try {
                secondaryProtocol.close();
            } finally {
                proxy.lock.unlock();
            }

            if (setSecondaryHostFail()) {
                addToBlacklist(this.secondaryProtocol.getHostAddress());
            }
        }
        return false;
    }

    /**
     * To handle the newly detected failover on the secondary connection.
     *
     * @param method the initial called method
     * @param args   the initial args
     * @return an object to indicate if the previous Exception must be thrown, or the object resulting if a failover worked
     * @throws Throwable if failover has not catch error
     */
    public HandleErrorResult secondaryFail(Method method, Object[] args) throws Throwable {
        if (tryPingOnSecondary()) {
            return relaunchOperation(method, args);
        }

        if (!isMasterHostFail()) {
            try {
                if (masterProtocol != null) {
                    this.masterProtocol.ping(); //check that master is on before switching to him
                    //switching to master connection
                    syncConnection(secondaryProtocol, masterProtocol);
                    proxy.lock.lock();
                    try {
                        currentProtocol = masterProtocol;
                    } finally {
                        proxy.lock.unlock();
                    }
                    launchFailLoopIfNotLaunched(false); //launch reconnection loop
                    return relaunchOperation(method, args); //now that we are on master, relaunched result if the result was not crashing the master
                }
            } catch (Exception e) {
                //ping fail on master
                if (setMasterHostFail()) {
                    addToBlacklist(masterProtocol.getHostAddress());
                    if (masterProtocol.isConnected()) {
                        proxy.lock.lock();
                        try {
                            masterProtocol.close();
                        } finally {
                            proxy.lock.unlock();
                        }
                    }
                }
            }
        }

        try {
            reconnectFailedConnection(new SearchFilter(true, true, true, true));
            handleFailLoop(false);
            if (isSecondaryHostFail()) {
                syncConnection(this.secondaryProtocol, this.masterProtocol);
                proxy.lock.lock();
                try {
                    currentProtocol = this.masterProtocol;
                } finally {
                    proxy.lock.unlock();
                }
            }
            return relaunchOperation(method, args); //now that we are reconnect, relaunched result if the result was not crashing the node
        } catch (Exception ee) {
            //we will throw a Connection exception that will close connection
            stopFailover();
            return new HandleErrorResult();
        }
    }

    /**
     * Check master status.
     * @param searchFilter search filter
     * @return has some status changed
     * @throws QueryException exception
     */
    public boolean checkMasterStatus(SearchFilter searchFilter) throws QueryException {
        if (masterProtocol != null) {
            masterProtocol.ping();
        }
        return false;
    }

    /**
     * private class to chech of currents connections are still ok.
     */
    protected class PingLoop extends TerminatableRunnable {
        MastersSlavesListener listener;

        public PingLoop(MastersSlavesListener listener) {
            this.listener = listener;
        }

        public void scheduleSelf(ScheduledExecutorService scheduler, long scheduleMillis) {
            if (scheduleState.compareAndSet(0, 1)) {
                scheduledFuture = scheduler.scheduleWithFixedDelay(this, scheduleMillis, scheduleMillis,
                        TimeUnit.MILLISECONDS);
            }
        }

        @Override
        protected void doRun() {
            if (!explicitClosed) {
                long durationSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - lastQueryNanos);
                if (durationSeconds >= urlParser.getOptions().validConnectionTimeout
                    && !isMasterHostFail()) {
                    boolean masterFail = false;
                    try {
                        if (masterProtocol != null && masterProtocol.isConnected()) {
                            checkMasterStatus(null);
                        } else {
                            masterFail = true;
                        }
                    } catch (QueryException e) {
                        masterFail = true;
                    }
    
                    if (masterFail && setMasterHostFail()) {
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

    protected FailLoop handleFailLoop(boolean now) {
        if (isMasterHostFail() || isSecondaryHostFail()) {
            if (!isExplicitClosed()) {
                launchFailLoopIfNotLaunched(now);
            }
        } else {
            return stopFailover();
        }
        return null;
    }
}
