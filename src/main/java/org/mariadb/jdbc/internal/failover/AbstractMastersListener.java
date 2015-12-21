package org.mariadb.jdbc.internal.failover;

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
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.util.SchedulerServiceProviderHolder;
import org.mariadb.jdbc.internal.util.SchedulerServiceProviderHolder.SchedulerProvider;
import org.mariadb.jdbc.internal.util.dao.QueryException;
import org.mariadb.jdbc.internal.query.MariaDbQuery;
import org.mariadb.jdbc.internal.query.Query;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.failover.tools.SearchFilter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;


public abstract class AbstractMastersListener implements Listener {

    /**
     * List the recent failedConnection.
     */
    protected static ConcurrentMap<HostAddress, Long> blacklist = new ConcurrentHashMap<>();
    /* =========================== Failover variables ========================================= */
    public final UrlParser urlParser;
    protected AtomicInteger currentConnectionAttempts = new AtomicInteger();
    protected AtomicBoolean currentReadOnlyAsked = new AtomicBoolean();
    private AtomicReference<FailLoop> runningFailLoop = new AtomicReference<>();
    protected Protocol currentProtocol = null;
    protected FailoverProxy proxy;
    protected long lastRetry = 0;
    protected boolean explicitClosed = false;
    private final SchedulerProvider schedulerProvider = SchedulerServiceProviderHolder.getSchedulerProvider();
    protected final ScheduledExecutorService scheduler = schedulerProvider.getScheduler(2);
    private volatile long masterHostFailNanos = 0;
    private AtomicBoolean masterHostFail = new AtomicBoolean();

    protected AbstractMastersListener(UrlParser urlParser) {
        this.urlParser = urlParser;
        this.masterHostFail.set(true);
    }
    
    protected void shutdownScheduler() {
        schedulerProvider.shutdownScheduler(scheduler);
        FailLoop runningLoop = stopFailover();
        if (runningLoop != null) {
            runningLoop.blockTillTerminated();
        }
    }
    
    @Override
    protected void finalize() throws Throwable {
        if (! scheduler.isShutdown()) {
            // TODO - log warning that scheduler was not cleaned up?
            // if we did not shutdown here, these threads would be leaked and never shutdown
            shutdownScheduler();
        }
        super.finalize();
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

    /**
     * Call when a failover is detected on master connection.
     * Will  : <ol>
     * <li> set fail variable</li>
     * <li> try to reconnect</li>
     * <li> relaunch query if possible</li>
     * </ol>
     *
     * @param method called method
     * @param args methods parameters
     * @return a HandleErrorResult object to indicate if query has been relaunched, and the exception if not
     * @throws Throwable when method and parameters does not exist.
     */
    public HandleErrorResult handleFailover(Method method, Object[] args) throws Throwable {
        if (explicitClosed) {
            throw new QueryException("Connection has been closed !");
        }
        if (setMasterHostFail()) {
//            log.warn("SQL Primary node [" + this.currentProtocol.getHostAddress().toString() + "] connection fail ");
            addToBlacklist(currentProtocol.getHostAddress());
        }
        return primaryFail(method, args);
    }

    /**
     * After a failover, put the hostAddress in a static list so the other connection will not take this host in account for a time.
     *
     * @param hostAddress the HostAddress to add to blacklist
     */
    public void addToBlacklist(HostAddress hostAddress) {
        if (hostAddress != null && !explicitClosed) {
            blacklist.put(hostAddress, System.nanoTime());
        }
    }

    /**
     * Permit to remove Host to blacklist after loadBalanceBlacklistTimeout seconds.
     */
    public void resetOldsBlackListHosts() {
        long currentTimeNanos = System.nanoTime();
        for (Map.Entry<HostAddress, Long> blEntry : blacklist.entrySet()) {
            long entryNanos = blEntry.getValue();
            long durationSeconds = TimeUnit.NANOSECONDS.toSeconds(currentTimeNanos - entryNanos);
            if (durationSeconds >= urlParser.getOptions().loadBalanceBlacklistTimeout) {
//              if (log.isTraceEnabled()) log.trace("host " + blackListHost+" remove of blacklist");
                blacklist.remove(blEntry.getKey(), entryNanos);
            }
        }
    }

    protected void resetMasterFailoverData() {
        if (masterHostFail.compareAndSet(true, false)) {
            masterHostFailNanos = 0;
        }
    }

    protected void setSessionReadOnly(boolean readOnly, Protocol protocol) throws QueryException {
        if (protocol.versionGreaterOrEqual(10, 0, 0)) {
            protocol.executeQuery(new MariaDbQuery("SET SESSION TRANSACTION " + (readOnly ? "READ ONLY" : "READ WRITE")));
        }
    }

    protected FailLoop stopFailover() {
        FailLoop loop;
        while ((loop = runningFailLoop.get()) != null) {
            if (runningFailLoop.compareAndSet(loop, null)) {
                // compare and swap guards that only one thread invokes unschedule on a specific loop instance
                loop.unscheduleTask();
                return loop;
            }
        }
        return null;
    }

    /**
     * launch the scheduler loop every 250 milliseconds, to reconnect a failed connection.
     * Will verify if there is an existing scheduler
     *
     * @param now now will launch the loop immediatly, 250ms after if false
     */
    protected void launchFailLoopIfNotlaunched(boolean now) {
        if (runningFailLoop.get() == null && urlParser.getOptions().failoverLoopRetries != 0) {
            FailLoop loopRunner = new FailLoop(this);
            if (runningFailLoop.compareAndSet(null, loopRunner)) {
                loopRunner.scheduleSelf(scheduler, now);
            }
        }
    }

    public Protocol getCurrentProtocol() {
        return currentProtocol;
    }

    public long getMasterHostFailNanos() {
        return masterHostFailNanos;
    }

    /**
     * Set master fail variables.
     * @return true if was already failed
     */
    public boolean setMasterHostFail() {
        if (masterHostFail.compareAndSet(false, true)) {
            masterHostFailNanos = System.nanoTime();
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
                String query = ((Query) args[0]).toString().toUpperCase();
                if (!query.equals("ALTER SYSTEM CRASH")
                        && !query.startsWith("KILL")) {
                    handleErrorResult.resultObject = method.invoke(currentProtocol, args);
                    handleErrorResult.mustThrowError = false;
                }
            } else if ("executePreparedQuery".equals(method.getName())) {
                //the statementId has been discarded with previous session
                try {
                    Method methodFailure = currentProtocol.getClass().getDeclaredMethod("executePreparedQueryAfterFailover",
                            String.class, ParameterHolder[].class, boolean.class);
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
     * When switching between 2 connections, report existing connection parameter to the new used connection.
     *
     * @param from used connection
     * @param to   will-be-current connection
     * @throws QueryException if catalog cannot be set
     */
    public void syncConnection(Protocol from, Protocol to) throws QueryException {

        if (from != null) {
            proxy.lock.lock();

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
                    to.executeQuery(new MariaDbQuery("set autocommit=" + (from.getAutocommit() ? "1" : "0")));
                }
            } finally {
                proxy.lock.unlock();
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
        return urlParser.getOptions().retriesAllDown;
    }

    public boolean isAutoReconnect() {
        return urlParser.getOptions().autoReconnect;
    }

    public UrlParser getUrlParser() {
        return urlParser;
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
    
    protected abstract static class TerminatableRunnable implements Runnable {
        private final AtomicInteger runState = new AtomicInteger(0); // -1 = removed, 0 = idle, 1 = active
        
        protected abstract void unscheduleTask();
        
        protected abstract void doRun();
        
        @Override
        public final void run() {
            if (! runState.compareAndSet(0, 1)) {
                // task has somehow either started to run in parallel (should not be possible)
                // or more likely the task has now been set to terminate
                return;
            }
            try {
                doRun();
            } finally {
                runState.compareAndSet(1, 0);
            }
        }
        
        public void blockTillTerminated() {
            unscheduleTask();
            while (! runState.compareAndSet(0, -1)) {
                // wait and retry
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                if (Thread.currentThread().isInterrupted()) {
                    runState.set(-1);
                    return;
                }
            }
        }
    }

    /**
     * Private class to permit a timer reconnection loop.
     */
    protected class FailLoop extends TerminatableRunnable {
        private volatile ScheduledFuture<?> scheduledFuture = null;
        Listener listener;

        public FailLoop(Listener listener) {
//            log.trace("launched FailLoop");
            this.listener = listener;
        }

        public void scheduleSelf(ScheduledExecutorService scheduler, boolean now) {
            scheduledFuture = scheduler.scheduleWithFixedDelay(this, now ? 0 : 250, 250, 
                                                               TimeUnit.MILLISECONDS);
        }

        // while scheduledFuture may be set from another thread
        // it is expected only one thread will unschedule
        @Override
        public void unscheduleTask() {
            while (true) {
                if (scheduledFuture == null) {
                    // not scheduled yet, should be a rare condition, just loop
                    Thread.yield();
                } else {
                    scheduledFuture.cancel(false);
                    scheduledFuture = null;
                    return;
                }
            }
        }

        @Override
        protected void doRun() {
            if (!explicitClosed && hasHostFail()) {
                if (listener.shouldReconnect()) {
                    try {
                        if (currentConnectionAttempts.get() >= urlParser.getOptions().failoverLoopRetries) {
                            throw new QueryException("Too many reconnection attempts (" + urlParser.getOptions().retriesAllDown + ")");
                        }
                        SearchFilter filter = getFilterForFailedHost();
                        filter.setUniqueLoop(true);
                        listener.reconnectFailedConnection(filter);
                        //reconnection done !
                        stopFailover();
                    } catch (Exception e) {
                        //FailLoop search connection failed
                    }
                } else {
                    if (currentConnectionAttempts.get() > urlParser.getOptions().retriesAllDown) {
                        //stopping failover after too many attemps
                        stopFailover();
                    }
                }
            } else {
                stopFailover();
            }
        }
    }

    public static void clearBlacklist() {
        blacklist.clear();
    }
}
