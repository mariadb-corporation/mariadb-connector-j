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

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.JDBCUrl;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.query.MySQLQuery;
import org.mariadb.jdbc.internal.common.query.Query;
import org.mariadb.jdbc.internal.common.query.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.mysql.FailoverProxy;
import org.mariadb.jdbc.internal.mysql.HandleErrorResult;
import org.mariadb.jdbc.internal.mysql.Protocol;
import org.mariadb.jdbc.internal.mysql.listener.tools.SearchFilter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public abstract class AbstractMastersListener implements Listener {

    /**
     * list the recent failedConnection
     */
    protected static Map<HostAddress, Long> blacklist = new HashMap<>();
    /* =========================== Failover variables ========================================= */
    public final JDBCUrl jdbcUrl;
    protected AtomicInteger currentConnectionAttempts = new AtomicInteger();
    protected AtomicBoolean currentReadOnlyAsked = new AtomicBoolean();
    protected AtomicBoolean isLooping = new AtomicBoolean();
    protected ScheduledFuture scheduledFailover = null;
    protected Protocol currentProtocol = null;
    protected FailoverProxy proxy;
    protected long lastRetry = 0;
    protected boolean explicitClosed = false;
    protected ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private AtomicLong masterHostFailTimestamp = new AtomicLong();
    private AtomicBoolean masterHostFail = new AtomicBoolean();

    protected AbstractMastersListener(JDBCUrl jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        this.masterHostFail.set(true);
    }

    public FailoverProxy getProxy() {
        return this.proxy;
    }

    public void setProxy(FailoverProxy proxy) {
        this.proxy = proxy;
    }

    public Map<HostAddress, Long> getBlacklist() {
        return blacklist;
    }

    public HandleErrorResult handleFailover(Method method, Object[] args) throws Throwable {
        if (explicitClosed) throw new QueryException("Connection has been closed !");
        if (setMasterHostFail()) {
//            log.warn("SQL Primary node [" + this.currentProtocol.getHostAddress().toString() + "] connection fail ");
            addToBlacklist(currentProtocol.getHostAddress());
        }
        return primaryFail(method, args);
    }

    /**
     * After a failover, put the hostAddress in a static list so the other connection will not take this host in account for a time
     *
     * @param hostAddress the HostAddress to add to blacklist
     */
    public void addToBlacklist(HostAddress hostAddress) {
        if (hostAddress != null) {
//            if (log.isTraceEnabled())log.trace("host " + hostAddress+" added to blacklist");
            blacklist.put(hostAddress, System.currentTimeMillis());
        }
    }

    /**
     * Permit to remove Host to blacklist after loadBalanceBlacklistTimeout seconds
     */
    public void resetOldsBlackListHosts() {
        long currentTime = System.currentTimeMillis();
        Set<HostAddress> currentBlackListkeys = new HashSet<HostAddress>(blacklist.keySet());
        for (HostAddress blackListHost : currentBlackListkeys) {
            if (blacklist.get(blackListHost) < currentTime - jdbcUrl.getOptions().loadBalanceBlacklistTimeout * 1000) {
//                if (log.isTraceEnabled()) log.trace("host " + blackListHost+" remove of blacklist");
                blacklist.remove(blackListHost);
            }
        }
    }

    protected void resetMasterFailoverData() {
        if (masterHostFail.compareAndSet(true, false)) masterHostFailTimestamp.set(0);
    }

    protected void setSessionReadOnly(boolean readOnly) throws QueryException {
        if (this.currentProtocol.versionGreaterOrEqual(10, 0, 0)) {
            this.currentProtocol.executeQuery(new MySQLQuery("SET SESSION TRANSACTION " + (readOnly ? "READ ONLY" : "READ WRITE")));
        }
    }

    protected void stopFailover() {
        if (isLooping.compareAndSet(true, false)) {
//            log.trace("stopping failover");
            if (scheduledFailover != null) scheduledFailover.cancel(false);
        }
    }

    /**
     * launch the scheduler loop every 250 milliseconds, to reconnect a failed connection.
     * Will verify if there is an existing scheduler
     *
     * @param now now will launch the loop immediatly, 250ms after if false
     */
    protected void launchFailLoopIfNotlaunched(boolean now) {
        if (isLooping.compareAndSet(false, true) && jdbcUrl.getOptions().failoverLoopRetries != 0) {
            scheduledFailover = Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new FailLoop(this), now ? 0 : 250, 250, TimeUnit.MILLISECONDS);
        }
    }

    public Protocol getCurrentProtocol() {
        return currentProtocol;
    }

    public long getMasterHostFailTimestamp() {
        return masterHostFailTimestamp.get();
    }

    public boolean setMasterHostFail() {
        if (masterHostFail.compareAndSet(false, true)) {
            masterHostFailTimestamp.set(System.currentTimeMillis());
            currentConnectionAttempts.set(0);
            return true;
        }
        return false;
    }

    public boolean isMasterHostFail() {
        return masterHostFail.get();
    }

    public boolean hasHostFail() {
        return masterHostFail.get();
    }

    public SearchFilter getFilterForFailedHost() {
        return new SearchFilter(isMasterHostFail(), false);
    }

    /**
     * After a failover that has bean done, relaunche the operation that was in progress.
     * In case of special operation that crash serveur, doesn't relaunched it;
     *
     * @param method the methode accessed
     * @param args   the parameters
     * @return An object that indicate the result or that the exception as to be thrown
     * @throws IllegalAccessException    if the initial call is not permit
     * @throws InvocationTargetException if there is any error relaunching initial method
     */
    public HandleErrorResult relaunchOperation(Method method, Object[] args) throws IllegalAccessException, InvocationTargetException {
        HandleErrorResult handleErrorResult = new HandleErrorResult(true);
        if (method != null) {
            if ("executeQuery".equals(method.getName())) {
                String query = ((Query) args[0]).getQuery().toUpperCase();
                if (!query.equals("ALTER SYSTEM CRASH")
                        && !query.startsWith("KILL")) {
                    handleErrorResult.resultObject = method.invoke(currentProtocol, args);
                    handleErrorResult.mustThrowError = false;
                }
            } else if ("executePreparedQuery".equals(method.getName())) {
                //the statementId has been discarded with previous session
                try {
                    Method methodFailure = currentProtocol.getClass().getDeclaredMethod("executePreparedQueryAfterFailover", String.class, ParameterHolder[].class, boolean.class);
                    handleErrorResult.resultObject = methodFailure.invoke(currentProtocol, args);
                    handleErrorResult.mustThrowError = false;
                } catch (Exception e) {
                }
            } else {
                handleErrorResult.resultObject = method.invoke(currentProtocol, args);
                handleErrorResult.mustThrowError = false;
            }
        }
        return handleErrorResult;
    }

    public Object invoke(Method method, Object[] args) throws Throwable {
        return method.invoke(currentProtocol, args);
    }

    /**
     * when switching between 2 connections, report existing connection parameter to the new used connection
     *
     * @param from used connection
     * @param to   will-be-current connection
     * @throws QueryException if catalog cannot be set
     */
    public void syncConnection(Protocol from, Protocol to) throws QueryException {

        if (from != null) {
            proxy.lock.writeLock().lock();

            try {
                to.setMaxRows(from.getMaxRows());
                to.setInternalMaxRows(from.getMaxRows());
                if (from.getTransactionIsolationLevel() != 0) {
                    to.setTransactionIsolation(from.getTransactionIsolationLevel());
                }
                if (from.getDatabase() != null && !"".equals(from.getDatabase()) && !from.getDatabase().equals(to.getDatabase())) {
                    to.setCatalog(from.getDatabase());
                }
                if (from.getAutocommit() != to.getAutocommit()) {
                    to.executeQuery(new MySQLQuery("set autocommit=" + (from.getAutocommit() ? "1" : "0")));
                }
            } finally {
                proxy.lock.writeLock().unlock();
            }

        }
    }

    public boolean isClosed() {
        return currentProtocol.isClosed();
    }

    public boolean isReadOnly() {
        return currentReadOnlyAsked.get();
    }

    public boolean isExplicitClosed() {
        return explicitClosed;
    }

    public void setExplicitClosed(boolean explicitClosed) {
        this.explicitClosed = explicitClosed;
    }

    public int getRetriesAllDown() {
        return jdbcUrl.getOptions().retriesAllDown;
    }

    public boolean isAutoReconnect() {
        return jdbcUrl.getOptions().autoReconnect;
    }

    public JDBCUrl getJdbcUrl() {
        return jdbcUrl;
    }

    public abstract void initializeConnection() throws QueryException;

    public abstract void preExecute() throws QueryException;

    public abstract void preClose() throws SQLException;

    public abstract boolean shouldReconnect();

    public abstract void reconnectFailedConnection(SearchFilter filter) throws QueryException;

    public abstract void switchReadOnlyConnection(Boolean readonly) throws QueryException;

    public abstract HandleErrorResult primaryFail(Method method, Object[] args) throws Throwable;

    public abstract void throwFailoverMessage(QueryException queryException, boolean reconnected) throws QueryException;

    public abstract void reconnect() throws QueryException;

    /**
     * private class to permit a timer reconnection loop
     */
    protected class FailLoop implements Runnable {
        Listener listener;

        public FailLoop(Listener listener) {
//            log.trace("launched FailLoop");
            this.listener = listener;
        }

        public void run() {
            if (hasHostFail()) {
//                    if (log.isTraceEnabled()) log.trace("failLoop , listener.shouldReconnect() : "+listener.shouldReconnect());
                if (listener.shouldReconnect()) {
                    try {
                        if (currentConnectionAttempts.get() >= jdbcUrl.getOptions().failoverLoopRetries)
                            throw new QueryException("Too many reconnection attempts (" + jdbcUrl.getOptions().retriesAllDown + ")");
                        SearchFilter filter = getFilterForFailedHost();
                        filter.setUniqueLoop(true);
                        listener.reconnectFailedConnection(filter);
                        //reconnection done !
                        stopFailover();
                    } catch (Exception e) {
//                            log.trace("FailLoop search connection failed", e);
                    }
                } else {
                    if (currentConnectionAttempts.get() > jdbcUrl.getOptions().retriesAllDown) {
//                            if (log.isDebugEnabled()) log.debug("stopping failover after too many attemps ("+currentConnectionAttempts+")");
                        stopFailover();
                    }
                }
            } else {
                stopFailover();
            }
//                log.trace("end launched FailLoop");
        }
    }

}
