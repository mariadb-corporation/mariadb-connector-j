/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.failover.impl;

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.MariaDbStatement;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.failover.AbstractMastersSlavesListener;
import org.mariadb.jdbc.internal.failover.HandleErrorResult;
import org.mariadb.jdbc.internal.failover.thread.FailoverLoop;
import org.mariadb.jdbc.internal.failover.tools.SearchFilter;
import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.protocol.MastersSlavesProtocol;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.dao.ReconnectDuringTransactionException;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;
import org.mariadb.jdbc.internal.util.pool.GlobalStateInfo;
import org.mariadb.jdbc.internal.util.scheduler.DynamicSizedSchedulerInterface;
import org.mariadb.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mariadb.jdbc.internal.util.SqlStates.CONNECTION_EXCEPTION;


/**
 * this class handle the operation when multiple hosts.
 */
public class MastersSlavesListener extends AbstractMastersSlavesListener {
    private static final DynamicSizedSchedulerInterface dynamicSizedScheduler;
    private static final AtomicInteger listenerCount = new AtomicInteger();
    private static final Logger logger = LoggerFactory.getLogger(MastersSlavesListener.class);

    static {
        dynamicSizedScheduler = SchedulerServiceProviderHolder.getScheduler(1, "MariaDb-failover", 8);

        // fail loop scaling happens async and only from a single thread
        dynamicSizedScheduler.scheduleWithFixedDelay(new Runnable() {
            private final ArrayDeque<FailoverLoop> failoverLoops = new ArrayDeque<>(8);

            @Override
            public void run() {

                int desiredFailCount = Math.min(8, listenerCount.get() / 5 + 1);
                int countChange = desiredFailCount - failoverLoops.size();

                if (countChange != 0) {
                    dynamicSizedScheduler.setPoolSize(desiredFailCount);
                    if (countChange > 0) {
                        // start fail loops
                        for (; countChange > 0; countChange--) {
                            // loop is started in constructor
                            failoverLoops.add(new FailoverLoop(dynamicSizedScheduler));
                        }
                    } else {
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
            }
        }, 1, 2, TimeUnit.MINUTES);
    }

    protected Protocol masterProtocol;
    protected Protocol secondaryProtocol;

    /**
     * Initialisation.
     *
     * @param urlParser     connection string object.
     * @param globalInfo    server global variables information
     */
    public MastersSlavesListener(final UrlParser urlParser, final GlobalStateInfo globalInfo) {
        super(urlParser, globalInfo);
        listenerCount.incrementAndGet();
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
     * @throws SQLException if a connection error append.
     */
    @Override
    public void initializeConnection() throws SQLException {
        super.initializeConnection();
        try {
            reconnectFailedConnection(new SearchFilter(true));
        } catch (SQLException e) {
            //initializeConnection failed
            checkInitialConnection(e);
        }
    }

    @Override
    public boolean isClosed() {
        if (currentProtocol != null) return currentProtocol.isClosed();
        if (urlParser.getOptions().allowMasterDownConnection) return secondaryProtocol.isClosed();
        return false;
    }

    @Override
    public Object invoke(Method method, Object[] args) throws Throwable {
        if (currentProtocol == null) {
            //possible with option "allowMasterDownConnection" set and no master found.
            //must try to reconnect
            try {
                reconnectFailedConnection(new SearchFilter(true, false));
                handleFailLoop();

            } catch (SQLException e) {
                //stop failover, since we will throw a connection exception that will close the connection.
                FailoverLoop.removeListener(this);
                throw new InvocationTargetException(
                        new SQLNonTransientConnectionException("No master connection available (only read-only)\n"
                                + "(Possible because option allowMasterDownConnection is set)",
                                CONNECTION_EXCEPTION.getSqlState()));
            }

            if (!isMasterHostFail()) {
                //connection established, no need to send Exception !
                //switching to master connection
                try {
                    syncConnection(this.secondaryProtocol, this.masterProtocol);
                    currentProtocol = this.masterProtocol;
                    return method.invoke(currentProtocol, args);
                } catch (SQLException e) {
                    //switching to master connection failed
                    if (setMasterHostFail()) {
                        addToBlacklist(masterProtocol.getHostAddress());
                    }
                }
            }
            throw new InvocationTargetException(
                    new SQLNonTransientConnectionException("No master connection available (only read-only)\n"
                            + "(Possible because option allowMasterDownConnection is set)",
                            CONNECTION_EXCEPTION.getSqlState()));
        }
        return method.invoke(currentProtocol, args);
    }

    @Override
    public boolean versionGreaterOrEqual(int major, int minor, int patch) {
        Protocol protocol = (currentProtocol != null) ? currentProtocol : secondaryProtocol;
        return protocol.versionGreaterOrEqual(major, minor, patch);
    }

    @Override
    public boolean sessionStateAware() {
        Protocol protocol = (currentProtocol != null) ? currentProtocol : secondaryProtocol;
        return protocol.sessionStateAware();
    }

    @Override
    public String getCatalog() throws SQLException {
        return (currentProtocol != null) ? currentProtocol.getCatalog() : secondaryProtocol.getDatabase();
    }

    public boolean isMasterConnection() {
        return (currentProtocol != null) ? currentProtocol.isMasterConnection() : true;
    }

    /**
     * Get timeout (master connection possibly down).
     *
     * @return socket timeout in ms
     * @throws SocketException if socket exception
     */
    public int getTimeout() throws SocketException {
        if (currentProtocol != null) {
            return currentProtocol.getTimeout();
        }
        return ((urlParser.getOptions().socketTimeout == null) ? 0 : urlParser.getOptions().socketTimeout);
    }

    @Override
    public void prolog(long maxRows, MariaDbConnection connection, MariaDbStatement statement) throws SQLException {
        if (currentProtocol != null) currentProtocol.prolog(maxRows, true, connection, statement);
    }

    @Override
    public boolean noBackslashEscapes() {
        Protocol protocol = (currentProtocol != null) ? currentProtocol : secondaryProtocol;
        return protocol.noBackslashEscapes();
    }

    public long getServerThreadId() {
        if (currentProtocol == null) return -1L;
        return currentProtocol.getServerThreadId();
    }

    protected void checkInitialConnection(SQLException queryException) throws SQLException {
        Protocol waitingProtocol;
        if (isSecondaryHostFail()) {
            if ((waitingProtocol = waitNewSecondaryProtocol.getAndSet(null)) != null) {
                this.secondaryProtocol = waitingProtocol;
                if (urlParser.getOptions().assureReadOnly) {
                    setSessionReadOnly(true, this.secondaryProtocol);
                }
                if (currentReadOnlyAsked) currentProtocol = waitingProtocol;
                resetSecondaryFailoverData();
            }
        }
        if (isMasterHostFail()) {
            if ((waitingProtocol = waitNewMasterProtocol.getAndSet(null)) != null) {
                this.masterProtocol = waitingProtocol;
                if (!currentReadOnlyAsked || isSecondaryHostFail()) {
                    currentProtocol = waitingProtocol;
                }
                resetMasterFailoverData();
            }
        }

        if (this.masterProtocol == null || !this.masterProtocol.isConnected()) {
            setMasterHostFail();
            if (!(urlParser.getOptions().allowMasterDownConnection && secondaryProtocol != null)) {
                throwFailoverMessage(masterProtocol != null ? masterProtocol.getHostAddress() : null, true, queryException, false);
            }
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
     */
    public void preClose() {
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
    public void preAbort() {
        if (explicitClosed.compareAndSet(false, true)) {
            proxy.lock.lock();
            try {
                removeListenerFromSchedulers();

                //closing connections
                abortConnection(waitNewSecondaryProtocol.getAndSet(null));
                abortConnection(waitNewMasterProtocol.getAndSet(null));
                abortConnection(masterProtocol);
                abortConnection(secondaryProtocol);
            } finally {
                proxy.lock.unlock();
            }
        }
    }

    @Override
    public void preExecute() throws SQLException {
        lastQueryNanos = System.nanoTime();
        checkWaitingConnection();
        //if connection is closed or failed on slave
        if (this.currentProtocol != null
                && (this.currentProtocol.isClosed() || (!currentReadOnlyAsked && !currentProtocol.isMasterConnection()))) {
            preAutoReconnect();
        }
    }


    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (currentProtocol != null) {
            if (currentProtocol.isMasterConnection()) {
                boolean valid = currentProtocol.isValid(timeout);
                if (secondaryProtocol != null) {
                    //ping secondary protocol too to avoid any server timeout
                    try {
                        boolean secondValid = secondaryProtocol.isValid(timeout);
                        if (!valid && urlParser.getOptions().allowMasterDownConnection && secondValid) {
                            setMasterHostFail();
                            return true;
                        }
                    } catch (SQLException sqle) {
                        //eat
                    }
                }
                return valid;
            } else {
                boolean valid = currentProtocol.isValid(timeout);
                if (masterProtocol != null) {
                    //ping secondary protocol too to avoid any server timeout
                    try {
                        masterProtocol.isValid(timeout);
                    } catch (SQLException sqle) {
                        //eat
                    }
                }
                return valid;
            }
        }
        return false;
    }

    /**
     * Verify that there is waiting connection that have to replace failing one.
     * If there is replace failed connection with new one.
     *
     * @throws SQLException if error occur
     */
    public void checkWaitingConnection() throws SQLException {
        if (isSecondaryHostFail()) {
            proxy.lock.lock();
            try {
                Protocol waitingProtocol = waitNewSecondaryProtocol.getAndSet(null);
                if (waitingProtocol != null && pingSecondaryProtocol(waitingProtocol)) {
                    lockAndSwitchSecondary(waitingProtocol);
                }
            } finally {
                proxy.lock.unlock();
            }
        }

        if (isMasterHostFail()) {
            proxy.lock.lock();
            try {
                Protocol waitingProtocol = waitNewMasterProtocol.getAndSet(null);
                if (waitingProtocol != null && pingMasterProtocol(waitingProtocol)) {
                    lockAndSwitchMaster(waitingProtocol);
                }
            } finally {
                proxy.lock.unlock();
            }
        }
    }


    /**
     * Loop to connect.
     *
     * @throws SQLException if there is any error during reconnection
     */
    public void reconnectFailedConnection(SearchFilter searchFilter) throws SQLException {
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
        blacklistShuffle.retainAll(urlParser.getHostAddresses());
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
            //while permit to avoid case when succeeded creating a new Master connection
            //and ping master connection fail a few millissecond after,
            //resulting a masterConnection not initialized.
            do {
                MastersSlavesProtocol.loop(this, globalInfo, loopAddress, searchFilter);
                //close loop if all connection are retrieved
                if (!searchFilter.isFailoverLoop()) {
                    try {
                        checkWaitingConnection();
                    } catch (ReconnectDuringTransactionException e) {
                        //don't throw an exception for this specific exception
                    }
                }
            } while (searchFilter.isInitialConnection()
                && !( masterProtocol != null || (urlParser.getOptions().allowMasterDownConnection && secondaryProtocol != null)));

            if (searchFilter.isInitialConnection() && masterProtocol == null && currentReadOnlyAsked) {
                currentProtocol = this.secondaryProtocol;
                currentReadOnlyAsked = true;
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
     * <p>
     * <i>Lock must be set</i>
     *
     * @param newMasterProtocol new master connection
     * @throws ReconnectDuringTransactionException if there was an active transaction.
     */
    public void lockAndSwitchMaster(Protocol newMasterProtocol) throws ReconnectDuringTransactionException {
        if (masterProtocol != null && !masterProtocol.isClosed()) {
            masterProtocol.close();
        }

        if (!currentReadOnlyAsked || isSecondaryHostFail()) {
            //actually on a secondary read-only because master was unknown.
            //So select master as currentConnection
            if (currentProtocol != null) {
                try {
                    syncConnection(currentProtocol, newMasterProtocol);
                } catch (Exception e) {
                    //Some error append during connection parameter synchronisation
                }
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
     * @throws SQLException if switch failed
     */
    public void foundActiveSecondary(Protocol newSecondaryProtocol) throws SQLException {
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
     * @throws SQLException if an error occur during setting session read-only
     */
    public void lockAndSwitchSecondary(Protocol newSecondaryProtocol) throws SQLException {
        if (secondaryProtocol != null && !secondaryProtocol.isClosed()) {
            secondaryProtocol.close();
        }

        //if asked to be on read only connection, switching to this new connection
        if (currentReadOnlyAsked || (urlParser.getOptions().failOnReadOnly && !currentReadOnlyAsked && isMasterHostFail())) {
            if (currentProtocol == null) {
                try {
                    syncConnection(currentProtocol, newSecondaryProtocol);
                } catch (Exception e) {
                    //Some error append during connection parameter synchronisation
                }
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
     * @throws SQLException if operation hasn't change protocol
     */
    @Override
    public void switchReadOnlyConnection(Boolean mustBeReadOnly) throws SQLException {
        checkWaitingConnection();
        if (currentReadOnlyAsked != mustBeReadOnly) {
            proxy.lock.lock();
            try {
                // another thread updated state
                if (currentReadOnlyAsked == mustBeReadOnly) return;
                currentReadOnlyAsked = mustBeReadOnly;
                if (currentReadOnlyAsked) {
                    if (currentProtocol == null) {
                        //switching to secondary connection
                        currentProtocol = this.secondaryProtocol;
                    } else if (currentProtocol.isMasterConnection()) {
                        //must change to replica connection
                        if (!isSecondaryHostFail()) {
                            try {
                                //switching to secondary connection
                                syncConnection(this.masterProtocol, this.secondaryProtocol);
                                currentProtocol = this.secondaryProtocol;
                                //current connection is now secondary
                                return;
                            } catch (SQLException e) {
                                //switching to secondary connection failed
                                if (setSecondaryHostFail()) {
                                    addToBlacklist(secondaryProtocol.getHostAddress());
                                }
                            }
                        }
                        //stay on master connection, since slave connection is fail
                        FailoverLoop.addListener(this);
                    }
                } else {
                    if (currentProtocol == null) {
                        //switching to master connection
                        currentProtocol = this.masterProtocol;
                    } else if (!currentProtocol.isMasterConnection()) {
                        //must change to master connection
                        if (!isMasterHostFail()) {
                            try {
                                //switching to master connection
                                syncConnection(this.secondaryProtocol, this.masterProtocol);
                                currentProtocol = this.masterProtocol;
                                //current connection is now master
                                return;
                            } catch (SQLException e) {
                                //switching to master connection failed
                                if (setMasterHostFail()) {
                                    addToBlacklist(masterProtocol.getHostAddress());
                                }
                            }
                        } else if (urlParser.getOptions().allowMasterDownConnection) {
                            currentProtocol = null;
                            return;
                        }

                        try {
                            reconnectFailedConnection(new SearchFilter(true, false));
                            handleFailLoop();

                        } catch (SQLException e) {
                            //stop failover, since we will throw a connection exception that will close the connection.
                            FailoverLoop.removeListener(this);
                            HostAddress failHost = (this.masterProtocol != null) ? this.masterProtocol.getHostAddress() : null;
                            throwFailoverMessage(failHost, true, new SQLException("master connection failed"), false);
                        }

                        if (!isMasterHostFail()) {
                            //connection established, no need to send Exception !
                            //switching to master connection
                            try {
                                syncConnection(this.secondaryProtocol, this.masterProtocol);
                                currentProtocol = this.masterProtocol;
                            } catch (SQLException e) {
                                //switching to master connection failed
                                if (setMasterHostFail()) {
                                    addToBlacklist(masterProtocol.getHostAddress());
                                }
                            }
                        } else {
                            currentReadOnlyAsked = !mustBeReadOnly;
                            HostAddress failHost = (this.masterProtocol != null) ? this.masterProtocol.getHostAddress() : null;
                            throwFailoverMessage(failHost, true, new SQLException("master connection failed"), false);
                        }

                    }
                }
            } finally {
                proxy.lock.unlock();
            }
        }
    }

    /**
     * To handle the newly detected failover on the master connection.
     *
     * @param method    the initial called method
     * @param args      the initial args
     * @param killCmd   is the fail due to a KILL cmd
     * @return an object to indicate if the previous Exception must be thrown, or the object resulting if a failover worked
     * @throws Throwable if failover has not been catch
     */
    public HandleErrorResult primaryFail(Method method, Object[] args, boolean killCmd) throws Throwable {
        boolean alreadyClosed = masterProtocol == null || !masterProtocol.isConnected();
        boolean inTransaction = masterProtocol != null && masterProtocol.inTransaction();

        //in case of SocketTimeoutException due to having set socketTimeout, must force connection close
        if (masterProtocol != null && masterProtocol.isConnected()) masterProtocol.close();

        //fail on slave if parameter permit so
        if (urlParser.getOptions().failOnReadOnly && !isSecondaryHostFail()) {
            try {
                if (this.secondaryProtocol != null && this.secondaryProtocol.ping()) {
                    //switching to secondary connection
                    proxy.lock.lock();
                    try {
                        if (masterProtocol != null) {
                            syncConnection(masterProtocol, this.secondaryProtocol);
                        }
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
            if (currentProtocol != null) {
                if (killCmd) return new HandleErrorResult(true, false);

                if (currentReadOnlyAsked || alreadyClosed || !inTransaction && isQueryRelaunchable(method, args)) {
                    //connection was not in transaction

                    //can relaunch query
                    logger.info("Connection to master lost, new master {}, conn={} found"
                                    + ", query type permit to be re-execute on new server without throwing exception",
                            currentProtocol.getHostAddress(),
                            currentProtocol.getServerThreadId());
                    return relaunchOperation(method, args);
                }
                //throw Exception because must inform client, even if connection is reconnected
                return new HandleErrorResult(true);
            } else {
                setMasterHostFail();
                FailoverLoop.removeListener(this);
                return new HandleErrorResult();
            }
        } catch (Exception e) {
            //we will throw a Connection exception that will close connection
            if (e.getCause() != null
                    && proxy.hasToHandleFailover((SQLException) e.getCause())
                    && currentProtocol != null
                    && currentProtocol.isConnected()) {
                currentProtocol.close();
            }
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
     * @throws SQLException if reconnection has failed
     */
    public void reconnect() throws SQLException {
        SearchFilter filter;
        boolean inTransaction = false;
        if (currentReadOnlyAsked) {
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

    /**
     * Ping secondary protocol.
     * ! lock must be set !
     *
     * @param protocol socket to ping
     * @return true if ping is valid.
     */
    private boolean pingSecondaryProtocol(Protocol protocol) {
        try {
            if (protocol != null && protocol.isConnected() && protocol.ping()) {
                return true;
            }
        } catch (Exception e) {
            protocol.close();

            if (setSecondaryHostFail()) {
                addToBlacklist(protocol.getHostAddress());
            }
        }
        return false;
    }

    /**
     * To handle the newly detected failover on the secondary connection.
     *
     * @param method    the initial called method
     * @param args      the initial args
     * @param killCmd   is fail due to a KILL command
     * @return an object to indicate if the previous Exception must be thrown, or the object resulting if a failover worked
     * @throws Throwable if failover has not catch error
     */
    public HandleErrorResult secondaryFail(Method method, Object[] args, boolean killCmd) throws Throwable {
        proxy.lock.lock();
        try {
            if (pingSecondaryProtocol(this.secondaryProtocol)) {
                return relaunchOperation(method, args);
            }
        } finally {
            proxy.lock.unlock();
        }


        if (!isMasterHostFail()) {
            try {
                //check that master is on before switching to him
                if (masterProtocol != null && masterProtocol.isValid(1000)) {
                    //switching to master connection
                    syncConnection(secondaryProtocol, masterProtocol);
                    proxy.lock.lock();
                    try {
                        currentProtocol = masterProtocol;
                    } finally {
                        proxy.lock.unlock();
                    }
                    FailoverLoop.addListener(this);
                    logger.info("Connection to slave lost, using master connection"
                            + ", query is re-execute on master server without throwing exception");
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

            if (killCmd) return new HandleErrorResult(true, false);

            logger.info("Connection to slave lost, new slave {}, conn={} found"
                    + ", query is re-execute on new server without throwing exception",
                    currentProtocol.getHostAddress(),
                    currentProtocol.getServerThreadId());
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

    @Override
    public void rePrepareOnSlave(ServerPrepareResult oldServerPrepareResult, boolean mustBeOnMaster) throws SQLException {
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

        if (secondaryProtocol != null && !isSecondaryHostFail()) {
            //prepare on slave
            ServerPrepareResult serverPrepareResult = secondaryProtocol.prepare(oldServerPrepareResult.getSql(), mustBeOnMaster);

            //release prepare on master
            try {
                serverPrepareResult.getUnProxiedProtocol().releasePrepareStatement(serverPrepareResult);
            } catch (SQLException exception) {
                //released failed.
            }

            //replace prepare data
            oldServerPrepareResult.failover(serverPrepareResult.getStatementId(), secondaryProtocol);
        }
    }

    /**
     * List current connected HostAddress.
     *
     * @return hostAddress List.
     */
    public List<HostAddress> connectedHosts() {
        List<HostAddress> usedHost = new ArrayList<>();

        if (isMasterHostFail()) {
            Protocol masterProtocol = waitNewMasterProtocol.get();
            if (masterProtocol != null) usedHost.add(masterProtocol.getHostAddress());
        } else usedHost.add(masterProtocol.getHostAddress());

        if (isSecondaryHostFail()) {
            Protocol secondProtocol = waitNewSecondaryProtocol.get();
            if (secondProtocol != null) usedHost.add(secondProtocol.getHostAddress());
        } else usedHost.add(secondaryProtocol.getHostAddress());

        return usedHost;
    }

    /**
     * Reset state of master and slave connection.
     *
     * @throws SQLException if command fail.
     */
    public void reset() throws SQLException {

        if (!isMasterHostFail()) {
            masterProtocol.reset();
        }

        if (!isSecondaryHostFail()) {
            secondaryProtocol.reset();
        }
    }

}
