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
import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.failover.thread.ConnectionValidator;
import org.mariadb.jdbc.internal.util.ExceptionMapper;
import org.mariadb.jdbc.internal.util.dao.PrepareResult;
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
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public abstract class AbstractMastersListener implements Listener {

    /**
     * List the recent failedConnection.
     */
    private static final ConcurrentMap<HostAddress, Long> blacklist = new ConcurrentHashMap<>();
    private static final ConnectionValidator connectionValidationLoop = new ConnectionValidator();

    /* =========================== Failover variables ========================================= */
    public final UrlParser urlParser;
    protected AtomicInteger currentConnectionAttempts = new AtomicInteger();
    // currentReadOnlyAsked is volatile so can be queried without lock, but can only be updated when proxy.lock is locked
    protected volatile boolean currentReadOnlyAsked = false;
    protected Protocol currentProtocol = null;
    protected FailoverProxy proxy;
    protected long lastRetry = 0;
    protected AtomicBoolean explicitClosed = new AtomicBoolean(false);
    private volatile long masterHostFailNanos = 0;
    private AtomicBoolean masterHostFail = new AtomicBoolean();
    protected long lastQueryNanos = 0;

    protected AbstractMastersListener(UrlParser urlParser) {
        this.urlParser = urlParser;
        this.masterHostFail.set(true);
        this.lastQueryNanos = System.nanoTime();
    }

    /**
     * Initialize Listener.
     * This listener will be added to the connection validation loop according to option value so the connection
     * will be verified periodically. (Important for aurora, for other, connection pool often have this functionality)
     * @throws QueryException if any exception occur.
     */
    public void initializeConnection() throws QueryException {
        long connectionTimeoutMillis = TimeUnit.SECONDS.toMillis(urlParser.getOptions().validConnectionTimeout);
        lastQueryNanos = System.nanoTime();
        if (connectionTimeoutMillis > 0) {
            connectionValidationLoop.addListener(this, connectionTimeoutMillis);
        }
    }

    protected void removeListenerFromSchedulers() {
        connectionValidationLoop.removeListener(this);
    }

    protected void preAutoReconnect() throws QueryException {
        if (!isExplicitClosed()) {
            try {
                // save to local value in case updated while constructing SearchFilter
                boolean currentReadOnlyAsked = this.currentReadOnlyAsked;
                reconnectFailedConnection(new SearchFilter(!currentReadOnlyAsked, currentReadOnlyAsked));
            } catch (QueryException e) {
                //eat exception
            }
            handleFailLoop();
        } else {
            throw new QueryException("Connection is closed", (short) -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState());
        }
    }

    public FailoverProxy getProxy() {
        return this.proxy;
    }

    public void setProxy(FailoverProxy proxy) {
        this.proxy = proxy;
    }

    public Set<HostAddress> getBlacklistKeys() {
        return blacklist.keySet();
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
     * @param args   methods parameters
     * @return a HandleErrorResult object to indicate if query has been relaunched, and the exception if not
     * @throws Throwable when method and parameters does not exist.
     */
    public HandleErrorResult handleFailover(Method method, Object[] args) throws Throwable {
        if (isExplicitClosed()) {
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
        if (hostAddress != null && !isExplicitClosed()) {
            blacklist.putIfAbsent(hostAddress, System.nanoTime());
        }
    }

    /**
     * After a successfull connection, permit to remove a hostAddress from blacklist.
     *
     * @param hostAddress the host address tho be remove of blacklist
     */
    public void removeFromBlacklist(HostAddress hostAddress) {
        if (hostAddress != null) {
            blacklist.remove(hostAddress);
        }
    }

    /**
     * Permit to remove Host to blacklist after loadBalanceBlacklistTimeout seconds.
     */
    public void resetOldsBlackListHosts() {
        long currentTimeNanos = System.nanoTime();
        Set<Map.Entry<HostAddress, Long>> entries = blacklist.entrySet();
        for (Map.Entry<HostAddress, Long> blEntry : entries) {
            long entryNanos = blEntry.getValue();
            long durationSeconds = TimeUnit.NANOSECONDS.toSeconds(currentTimeNanos - entryNanos);
            if (durationSeconds >= urlParser.getOptions().loadBalanceBlacklistTimeout) {
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
        if (protocol.versionGreaterOrEqual(5, 6, 5)) {
            protocol.executeQuery(new MariaDbQuery("SET SESSION TRANSACTION " + (readOnly ? "READ ONLY" : "READ WRITE")));
        }
    }

    public abstract void handleFailLoop();

    public Protocol getCurrentProtocol() {
        return currentProtocol;
    }

    public long getMasterHostFailNanos() {
        return masterHostFailNanos;
    }

    /**
     * Set master fail variables.
     *
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
     * After a failover that has bean done, relaunch the operation that was in progress.
     * In case of special operation that crash server, doesn't relaunched it;
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
                            String.class, ParameterHolder[].class, PrepareResult.class, MariaDbType[].class, boolean.class);
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

    /**
     * Check if query can be re-executed.
     * @param method invoke method
     * @param args invoke arguments
     * @return true if can be re-executed
     */
    public boolean isQueryRelaunchable(Method method, Object[] args) {
        if (method != null && "executeQuery".equals(method.getName())) {
            if (args[0] instanceof Query) {
                return ((Query) args[0]).toString().toUpperCase().startsWith("SELECT");
            }
        }
        return false;
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
        return currentReadOnlyAsked;
    }

    public boolean isExplicitClosed() {
        return explicitClosed.get();
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

    public abstract void preExecute() throws QueryException;

    public abstract void preClose() throws SQLException;

    public abstract void reconnectFailedConnection(SearchFilter filter) throws QueryException;

    public abstract void switchReadOnlyConnection(Boolean readonly) throws QueryException;

    public abstract HandleErrorResult primaryFail(Method method, Object[] args) throws Throwable;

    /**
     * Throw a human readable message after a failoverException.
     *
     * @param failHostAddress failedHostAddress
     * @param wasMaster       was failed connection master
     * @param queryException  internal error
     * @param reconnected     connection status
     * @throws QueryException error with failover information
     */
    @Override
    public void throwFailoverMessage(HostAddress failHostAddress, boolean wasMaster, QueryException queryException,
                                     boolean reconnected) throws QueryException {
        String firstPart = "Communications link failure with "
                + (wasMaster ? "primary" : "secondary")
                + ((failHostAddress != null) ? " host " + failHostAddress.host + ":" + failHostAddress.port : "") + ". ";
        String error = "";
        if (reconnected) {
            error += " Driver has reconnect connection";
        } else {
            if (currentConnectionAttempts.get() > urlParser.getOptions().retriesAllDown) {
                error += " Driver will not try to reconnect (too much failure > " + urlParser.getOptions().retriesAllDown + ")";
            }
        }

        if (queryException == null) {
            queryException = new QueryException(firstPart + error, (short) -1, ExceptionMapper.SqlStates.CONNECTION_EXCEPTION.getSqlState());
        } else {
            error = queryException.getMessage() + ". " + error;
            queryException.setMessage(firstPart + error);
        }

        if (reconnected && queryException.getSqlState().startsWith("08")) {
            //change sqlState to "Transaction has been rolled back", to transaction exception, since reconnection has succeed
            queryException.setSqlState("25S03");
        }
        throw queryException;

    }

    public boolean canRetryFailLoop() {
        return currentConnectionAttempts.get() < urlParser.getOptions().failoverLoopRetries;
    }


    public abstract void reconnect() throws QueryException;

    public abstract boolean checkMasterStatus(SearchFilter searchFilter);


    /**
     * Clear blacklist data.
     */
    public static void clearBlacklist() {
        blacklist.clear();
    }

    public long getLastQueryNanos() {
        return lastQueryNanos;
    }

    protected boolean pingMasterProtocol(Protocol protocol) {
        try {
            protocol.ping();
            return true;
        } catch (QueryException e) {
            proxy.lock.lock();
            try {
                protocol.close();
                if (setMasterHostFail()) {
                    addToBlacklist(protocol.getHostAddress());
                }
            } finally {
                proxy.lock.unlock();
            }
        }
        return false;
    }

    /**
     * Utility to close existing connection.
     * @param protocol connection to close.
     */
    public void closeConnection(Protocol protocol) {
        if (protocol != null && protocol.isConnected()) {
            protocol.close();
        }
    }

}
