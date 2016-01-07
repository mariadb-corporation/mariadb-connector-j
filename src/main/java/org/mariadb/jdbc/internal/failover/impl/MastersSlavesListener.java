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
import org.mariadb.jdbc.internal.failover.thread.FailoverLoop;
import org.mariadb.jdbc.internal.util.dao.ReconnectDuringTransactionException;
import org.mariadb.jdbc.internal.util.scheduler.DynamicSizedSchedulerInterface;
import org.mariadb.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.failover.HandleErrorResult;
import org.mariadb.jdbc.internal.protocol.MastersSlavesProtocol;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.failover.tools.SearchFilter;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * this class handle the operation when multiple hosts.
 */
public class MastersSlavesListener extends AbstractMastersSlavesListener {
    private static final double POOL_SIZE_TO_LISTENER_RATIO = 0.3d;
    private static final double FAIL_LOOP_TO_LISTENER_RATIO = 0.3d;

    protected Protocol masterProtocol;
    protected Protocol secondaryProtocol;
    private static final DynamicSizedSchedulerInterface dynamicSizedScheduler;
    private static final AtomicInteger listenerCount = new AtomicInteger();

    static {
        dynamicSizedScheduler = SchedulerServiceProviderHolder.getScheduler(1);

        // pool decreases happen async
        dynamicSizedScheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                dynamicSizedScheduler.setPoolSize((int) Math.ceil(listenerCount.get() * POOL_SIZE_TO_LISTENER_RATIO));
            }
        }, 2, 2, TimeUnit.HOURS);

        // fail loop scaling happens async and only from a single thread
        dynamicSizedScheduler.scheduleWithFixedDelay(new Runnable() {
            private final ArrayDeque<FailoverLoop> failoverLoops = new ArrayDeque<>(8);

            @Override
            public void run() {
                int desiredFailCount = (int) Math.ceil(listenerCount.get() * FAIL_LOOP_TO_LISTENER_RATIO);
                int countChange = desiredFailCount - failoverLoops.size();
                if (countChange > 0) {
                    // start fail loops
                    for (; countChange > 0; countChange--) {
                        // loop is started in constructor
                        failoverLoops.add(new FailoverLoop(dynamicSizedScheduler));
                    }
                } else if (countChange < 0) {
                    // block on all removed loops after finished unscheduling to reduce blocking
                    List<FailoverLoop> removedLoops = new ArrayList<>(-countChange);
                    // terminate fail loops
                    for (; countChange < 0; countChange++) {
                        FailoverLoop failoverLoop = failoverLoops.remove();
                        failoverLoop.unscheduleTask();
                        removedLoops.add(failoverLoop);
                    }

                    for (FailoverLoop failoverLoop : removedLoops) {
                        failoverLoop.blockTillTerminated();
                    }
                }
            }
        }, 250, 250, TimeUnit.MILLISECONDS);
    }

    /**
     * Initialisation.
     *
     * @param urlParser connection string object.
     */
    public MastersSlavesListener(final UrlParser urlParser) {
        super(urlParser);
        dynamicSizedScheduler.setPoolSize((int) Math.ceil(listenerCount.incrementAndGet() * POOL_SIZE_TO_LISTENER_RATIO));
        masterProtocol = null;
        secondaryProtocol = null;
        setMasterHostFail();
        setSecondaryHostFail();
    }

    protected void removeListenerFromSchedulers() {
        super.removeListenerFromSchedulers();
        FailoverLoop.removeListener(this);
        listenerCount.addAndGet(-1);
    }

    /**
     * Initialize connections.
     *
     * @throws QueryException if a connection error append.
     */
    @Override
    public void initializeConnection() throws QueryException {
        super.initializeConnection();
        try {
            reconnectFailedConnection(new SearchFilter(true));
        } catch (QueryException e) {
            //initializeConnection failed
            checkInitialConnection(e);
        }
    }

    protected void checkInitialConnection(QueryException queryException) throws QueryException {
        if (this.secondaryProtocol == null || !this.secondaryProtocol.isConnected()) {
            setSecondaryHostFail();
        } else {
            resetSecondaryFailoverData();
        }

        if (this.masterProtocol == null || !this.masterProtocol.isConnected()) {
            setMasterHostFail();
            throwFailoverMessage(masterProtocol != null ? masterProtocol.getHostAddress() : null, true, queryException, false);
        } else {
            resetMasterFailoverData();
            if (isSecondaryHostFail()) {
                //launched failLoop only if not throwing connection (connection will be closed).
                handleFailLoop();
            }
        }
    }

    /**
     * Called after a call on Connection.close(). Will explicitly closed all connections.
     *
     * @throws SQLException if error append during closing those connections.
     */
    public void preClose() throws SQLException {
        if (explicitClosed.compareAndSet(false, true)) {
            proxy.lock.lock();
            try {
                removeListenerFromSchedulers();

                //closing connections
                closeConnection(waitNewSecondaryProtocol.getAndSet(null));
                closeConnection(waitNewMasterProtocol.getAndSet(null));
                closeConnection(masterProtocol);
                closeConnection(secondaryProtocol);
            } finally {
                proxy.lock.unlock();
            }
        }
    }

    @Override
    public void preExecute() throws QueryException {
        lastQueryNanos = System.nanoTime();
        checkWaitingConnection();
        //if connection is closed or failed on slave
        if (this.currentProtocol != null
                && (this.currentProtocol.isClosed() || (!currentReadOnlyAsked.get() && !currentProtocol.isMasterConnection()))) {
            preAutoReconnect();
        }
    }

    /**
     * Verify that there is waiting connection that have to replace failing one.
     * If there is replace failed connection with new one.
     *
     * @throws QueryException if error occur
     */
    public void checkWaitingConnection() throws QueryException {
        if (isSecondaryHostFail()) {
            Protocol waitingProtocol = waitNewSecondaryProtocol.getAndSet(null);
            if (waitingProtocol != null) {
                proxy.lock.lock();
                try {
                    if (pingSecondaryProtocol(waitingProtocol)) {
                        lockAndSwitchSecondary(waitingProtocol);
                    }
                } finally {
                    proxy.lock.unlock();
                }
            }
        }

        if (isMasterHostFail()) {
            Protocol waitingProtocol = waitNewMasterProtocol.getAndSet(null);
            if (waitingProtocol != null) {
                proxy.lock.lock();
                try {
                    if (pingMasterProtocol(waitingProtocol)) {
                        lockAndSwitchMaster(waitingProtocol);
                    }
                } finally {
                    proxy.lock.unlock();
                }
            }
        }
    }


    /**
     * Loop to connect.
     *
     * @throws QueryException if there is any error during reconnection
     */
    public void reconnectFailedConnection(SearchFilter searchFilter) throws QueryException {
        if (!searchFilter.isInitialConnection()
                && (isExplicitClosed()
                || (searchFilter.isFineIfFoundOnlyMaster() && !isMasterHostFail())
                || searchFilter.isFineIfFoundOnlySlave() && !isSecondaryHostFail())) {
            return;
        }
        //check if a connection has been retrieved by failoverLoop during lock
        if (!searchFilter.isFailoverLoop()) {
            try {
                checkWaitingConnection();
                if ((searchFilter.isFineIfFoundOnlyMaster() && !isMasterHostFail())
                        || searchFilter.isFineIfFoundOnlySlave() && !isSecondaryHostFail()) {
                    return;
                }
            } catch (ReconnectDuringTransactionException e) {
                //don't throw an exception for this specific exception
                return;
            }
        }


        currentConnectionAttempts.incrementAndGet();
        resetOldsBlackListHosts();

        //put the list in the following order
        // - random order not blacklist and not connected host
        // - random order blacklist host
        // - connected host
        List<HostAddress> loopAddress = new LinkedList<>(urlParser.getHostAddresses());
        loopAddress.removeAll(getBlacklistKeys());
        Collections.shuffle(loopAddress);
        List<HostAddress> blacklistShuffle = new LinkedList<>(getBlacklistKeys());
        Collections.shuffle(blacklistShuffle);
        loopAddress.addAll(blacklistShuffle);

        //put connected at end
        if (masterProtocol != null && !isMasterHostFail()) {
            loopAddress.remove(masterProtocol.getHostAddress());
            loopAddress.add(masterProtocol.getHostAddress());
        }

        if (secondaryProtocol != null && !isSecondaryHostFail()) {
            loopAddress.remove(secondaryProtocol.getHostAddress());
            loopAddress.add(secondaryProtocol.getHostAddress());
        }

        if ((isMasterHostFail() || isSecondaryHostFail())
                || searchFilter.isInitialConnection()) {
            MastersSlavesProtocol.loop(this, loopAddress, searchFilter);
            //close loop if all connection are retrieved
            if (!searchFilter.isFailoverLoop()) {
                try {
                    checkWaitingConnection();
                } catch (ReconnectDuringTransactionException e) {
                    //don't throw an exception for this specific exception
                }
            }
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
            if (!waitNewMasterProtocol.compareAndSet(null, newMasterProtocol)) {
                newMasterProtocol.close();
            }
        } else {
            newMasterProtocol.close();
        }

    }

    /**
     * Use the parameter newMasterProtocol as new current master connection.
     *
     * @param newMasterProtocol new master connection
     * @throws ReconnectDuringTransactionException if there was an active transaction.
     */
    public void lockAndSwitchMaster(Protocol newMasterProtocol) throws ReconnectDuringTransactionException {
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

        boolean inTransaction = this.masterProtocol != null && this.masterProtocol.inTransaction();
        this.masterProtocol = newMasterProtocol;
        resetMasterFailoverData();
        if (inTransaction) {
            //master connection was down, so has been change for a new active connection
            //problem was there was an active connection -> must throw exception so client known it
            throw new ReconnectDuringTransactionException("Connection reconnect automatically during an active transaction", 1401, "25S03");
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

            if (proxy.lock.tryLock()) {
                try {
                    lockAndSwitchSecondary(newSecondaryProtocol);
                } finally {
                    proxy.lock.unlock();
                }
            } else {
                if (!waitNewSecondaryProtocol.compareAndSet(null, newSecondaryProtocol)) {
                    newSecondaryProtocol.close();
                }
            }
        } else {
            newSecondaryProtocol.close();
        }
    }

    /**
     * Use the parameter newSecondaryProtocol as new current secondary connection.
     *
     * @param newSecondaryProtocol new secondary connection
     * @throws QueryException if an error occur during setting session read-only
     */
    public void lockAndSwitchSecondary(Protocol newSecondaryProtocol) throws QueryException {
        if (secondaryProtocol != null && !secondaryProtocol.isClosed()) {
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
        checkWaitingConnection();
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
                    FailoverLoop.addListener(this);
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
                        reconnectFailedConnection(new SearchFilter(true, false));
                        handleFailLoop();
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
                    } catch (QueryException e) {
                        //stop failover, since we will throw a connection exception that will close the connection.
                        FailoverLoop.removeListener(this);
                        HostAddress failHost = (this.masterProtocol != null) ? this.masterProtocol.getHostAddress() : null;
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
        boolean inTransaction = masterProtocol != null && masterProtocol.inTransaction();
        //try to reconnect automatically only time before looping
        try {
            if (masterProtocol != null && masterProtocol.isConnected() && masterProtocol.ping()) {
                if (inTransaction) {
                    masterProtocol.rollback();
                    return new HandleErrorResult(true);
                }
                return relaunchOperation(method, args);
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
                    FailoverLoop.addListener(this);
                    try {
                        return relaunchOperation(method, args);
                    } catch (Exception e) {
                        //relaunchOperation failed
                    }
                    return new HandleErrorResult();
                }
            } catch (Exception e) {
                if (setSecondaryHostFail()) {
                    blackListAndCloseConnection(this.secondaryProtocol);
                }
            }
        }

        try {
            reconnectFailedConnection(new SearchFilter(true, urlParser.getOptions().failOnReadOnly));
            handleFailLoop();
            if (currentReadOnlyAsked.get() //use master connection temporary in replacement of slave
                    || alreadyClosed //connection was already close
                    || (!alreadyClosed && !inTransaction && isQueryRelaunchable(method, args) )) { //connection was not in transaction

                    //can relaunch query
                return relaunchOperation(method, args);
            }
            //throw Exception because must inform client, even if connection is reconnected
            return new HandleErrorResult(true);
        } catch (Exception e) {
            //we will throw a Connection exception that will close connection
            setMasterHostFail();
            FailoverLoop.removeListener(this);
            return new HandleErrorResult();
        }
    }

    private void blackListAndCloseConnection(Protocol protocol) {
        addToBlacklist(protocol.getHostAddress());
        if (protocol.isConnected()) {
            proxy.lock.lock();
            try {
                protocol.close();
            } finally {
                proxy.lock.unlock();
            }
        }
    }

    /**
     * Reconnect failed connection.
     *
     * @throws QueryException if reconnection has failed
     */
    public void reconnect() throws QueryException {
        SearchFilter filter;
        boolean inTransaction = false;
        if (currentReadOnlyAsked.get()) {
            filter = new SearchFilter(true, true);
        } else {
            inTransaction = masterProtocol != null && masterProtocol.inTransaction();
            filter = new SearchFilter(true, urlParser.getOptions().failOnReadOnly);
        }
        reconnectFailedConnection(filter);
        handleFailLoop();
        if (inTransaction) {
            throw new ReconnectDuringTransactionException("Connection reconnect automatically during an active transaction", 1401, "25S03");
        }
    }

    private boolean pingSecondaryProtocol(Protocol protocol) {
        try {
            if (protocol != null && protocol.isConnected() && protocol.ping()) {
                return true;
            }
        } catch (Exception e) {
            proxy.lock.lock();
            try {
                protocol.close();
            } finally {
                proxy.lock.unlock();
            }

            if (setSecondaryHostFail()) {
                addToBlacklist(protocol.getHostAddress());
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
        if (pingSecondaryProtocol(this.secondaryProtocol)) {
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
                    FailoverLoop.addListener(this);
                    return relaunchOperation(method, args); //now that we are on master, relaunched result if the result was not crashing the master
                }
            } catch (Exception e) {
                //ping fail on master
                if (setMasterHostFail()) {
                    blackListAndCloseConnection(masterProtocol);
                }
            }
        }

        try {
            reconnectFailedConnection(new SearchFilter(true, true));
            handleFailLoop();
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
            FailoverLoop.removeListener(this);
            return new HandleErrorResult();
        }
    }

    @Override
    public void handleFailLoop() {
        if (isMasterHostFail() || isSecondaryHostFail()) {
            if (!isExplicitClosed()) {
                FailoverLoop.addListener(this);
            }
        } else {
            FailoverLoop.removeListener(this);
        }
    }

    @Override
    public boolean isMasterConnected() {
        return masterProtocol != null && masterProtocol.isConnected();
    }

    /**
     * Check master status.
     *
     * @param searchFilter search filter
     * @return has some status changed
     */
    @Override
    public boolean checkMasterStatus(SearchFilter searchFilter) {
        if (masterProtocol != null) {
            pingMasterProtocol(masterProtocol);
        }
        return false;
    }

}
