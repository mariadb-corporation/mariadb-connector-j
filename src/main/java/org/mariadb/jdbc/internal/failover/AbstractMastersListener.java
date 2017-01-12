package org.mariadb.jdbc.internal.failover;

/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.

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
import org.mariadb.jdbc.internal.failover.thread.ConnectionValidator;
import org.mariadb.jdbc.internal.failover.tools.SearchFilter;
import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.dao.ClientPrepareResult;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mariadb.jdbc.internal.util.SqlStates.CONNECTION_EXCEPTION;

public abstract class AbstractMastersListener implements Listener {

    /**
     * List the recent failedConnection.
     */
    private static final ConcurrentMap<HostAddress, Long> blacklist = new ConcurrentHashMap<>();
    private static final ConnectionValidator connectionValidationLoop = new ConnectionValidator();
    private static Logger logger = LoggerFactory.getLogger(AbstractMastersListener.class);

    /* =========================== Failover variables ========================================= */
    public final UrlParser urlParser;
    protected AtomicInteger currentConnectionAttempts = new AtomicInteger();
    // currentReadOnlyAsked is volatile so can be queried without lock, but can only be updated when proxy.lock is locked
    protected volatile boolean currentReadOnlyAsked = false;
    protected Protocol currentProtocol = null;
    protected FailoverProxy proxy;
    protected long lastRetry = 0;
    protected AtomicBoolean explicitClosed = new AtomicBoolean(false);
    protected long lastQueryNanos = 0;
    private volatile long masterHostFailNanos = 0;
    private AtomicBoolean masterHostFail = new AtomicBoolean();

    protected AbstractMastersListener(UrlParser urlParser) {
        this.urlParser = urlParser;
        this.masterHostFail.set(true);
        this.lastQueryNanos = System.nanoTime();
    }

    /**
     * Clear blacklist data.
     */
    public static void clearBlacklist() {
        blacklist.clear();
    }

    /**
     * Initialize Listener.
     * This listener will be added to the connection validation loop according to option value so the connection
     * will be verified periodically. (Important for aurora, for other, connection pool often have this functionality)
     *
     * @throws SQLException if any exception occur.
     */
    public void initializeConnection() throws SQLException {
        long connectionTimeoutMillis = TimeUnit.SECONDS.toMillis(urlParser.getOptions().validConnectionTimeout);
        lastQueryNanos = System.nanoTime();
        if (connectionTimeoutMillis > 0) {
            connectionValidationLoop.addListener(this, connectionTimeoutMillis);
        }
    }

    protected void removeListenerFromSchedulers() {
        connectionValidationLoop.removeListener(this);
    }

    protected void preAutoReconnect() throws SQLException {
        if (!isExplicitClosed()) {
            try {
                // save to local value in case updated while constructing SearchFilter
                boolean currentReadOnlyAsked = this.currentReadOnlyAsked;
                reconnectFailedConnection(new SearchFilter(!currentReadOnlyAsked, currentReadOnlyAsked));
            } catch (SQLException e) {
                //eat exception
            }
            handleFailLoop();
        } else {
            throw new SQLException("Connection is closed", CONNECTION_EXCEPTION.getSqlState());
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
     * @param method   called method
     * @param args     methods parameters
     * @param protocol current protocol
     * @return a HandleErrorResult object to indicate if query has been relaunched, and the exception if not
     * @throws Throwable when method and parameters does not exist.
     */
    public HandleErrorResult handleFailover(SQLException qe, Method method, Object[] args, Protocol protocol) throws Throwable {
        if (isExplicitClosed()) {
            throw new SQLException("Connection has been closed !");
        }
        if (setMasterHostFail()) {
            logger.warn("SQL Primary node [" + this.currentProtocol.getHostAddress().toString()
                    + ", conn " + this.currentProtocol.getServerThreadId()
                    + " ] connection fail. Reason : " + qe.getMessage());
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

    protected void setSessionReadOnly(boolean readOnly, Protocol protocol) throws SQLException {
        if (protocol.versionGreaterOrEqual(5, 6, 5)) {
            protocol.executeQuery("SET SESSION TRANSACTION " + (readOnly ? "READ ONLY" : "READ WRITE"));
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
            switch (method.getName()) {
                case "executeQuery":
                    if (args[2] instanceof String) {
                        String query = ((String) args[2]).toUpperCase();
                        if (!query.equals("ALTER SYSTEM CRASH")
                                && !query.startsWith("KILL")) {
                            logger.debug("relaunch query to new connection "
                                    + ((currentProtocol != null) ? "server thread id " + currentProtocol.getServerThreadId() : ""));
                            handleErrorResult.resultObject = method.invoke(currentProtocol, args);
                            handleErrorResult.mustThrowError = false;
                        }
                    }
                    break;
                case "executePreparedQuery":
                    //the statementId has been discarded with previous session
                    try {
                        boolean mustBeOnMaster = (Boolean) args[0];
                        ServerPrepareResult oldServerPrepareResult = (ServerPrepareResult) args[1];
                        ServerPrepareResult serverPrepareResult = currentProtocol.prepare(oldServerPrepareResult.getSql(), mustBeOnMaster);
                        oldServerPrepareResult.failover(serverPrepareResult.getStatementId(), currentProtocol);
                        logger.debug("relaunch query to new connection "
                                + ((currentProtocol != null) ? "server thread id " + currentProtocol.getServerThreadId() : ""));
                        handleErrorResult.resultObject = method.invoke(currentProtocol, args);
                        handleErrorResult.mustThrowError = false;
                    } catch (Exception e) {
                    }
                    break;
                default:
                    handleErrorResult.resultObject = method.invoke(currentProtocol, args);
                    handleErrorResult.mustThrowError = false;
            }
        }
        return handleErrorResult;
    }

    /**
     * Check if query can be re-executed.
     *
     * @param method invoke method
     * @param args   invoke arguments
     * @return true if can be re-executed
     */
    public boolean isQueryRelaunchable(Method method, Object[] args) {
        if (method != null) {
            switch (method.getName()) {
                case "executeQuery":
                    if (!((Boolean) args[0])) return true; //launched on slave connection
                    if (args[2] instanceof String) {
                        return ((String) args[2]).toUpperCase().startsWith("SELECT");
                    } else if (args[2] instanceof ClientPrepareResult) {
                        @SuppressWarnings("unchecked")
                        String query = new String(((ClientPrepareResult) args[2]).getQueryParts().get(0)).toUpperCase();
                        return query.startsWith("SELECT");
                    }
                    break;
                case "executePreparedQuery":
                    if (!((Boolean) args[0])) return true; //launched on slave connection
                    ServerPrepareResult serverPrepareResult = (ServerPrepareResult) args[1];
                    return (serverPrepareResult.getSql()).toUpperCase().startsWith("SELECT");
                case "prepareAndExecute":
                    if (!((Boolean) args[0])) return true; //launched on slave connection
                    return ((String) args[2]).toUpperCase().startsWith("SELECT");
                case "executeBatch":
                case "executeBatchMultiple":
                case "executeBatchRewrite":
                case "prepareAndExecutes":
                case "executeBatchMulti":
                    if (!((Boolean) args[0])) return true; //launched on slave connection
                    return false;
                default:
                    return false;
            }
        }
        return false;
    }

    public Object invoke(Method method, Object[] args, Protocol specificProtocol) throws Throwable {
        return method.invoke(specificProtocol, args);
    }

    public Object invoke(Method method, Object[] args) throws Throwable {
        return method.invoke(currentProtocol, args);
    }

    /**
     * When switching between 2 connections, report existing connection parameter to the new used connection.
     *
     * @param from used connection
     * @param to   will-be-current connection
     * @throws SQLException if catalog cannot be set
     */
    public void syncConnection(Protocol from, Protocol to) throws SQLException {

        if (from != null) {

            proxy.lock.lock();
            try {
                to.resetStateAfterFailover(from.getMaxRows(), from.getTransactionIsolationLevel(), from.getDatabase(), from.getAutocommit());
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

    public abstract void preExecute() throws SQLException;

    public abstract void preClose() throws SQLException;

    public abstract void reconnectFailedConnection(SearchFilter filter) throws SQLException;

    public abstract void switchReadOnlyConnection(Boolean readonly) throws SQLException;

    public abstract HandleErrorResult primaryFail(Method method, Object[] args) throws Throwable;

    /**
     * Throw a human readable message after a failoverException.
     *
     * @param failHostAddress failedHostAddress
     * @param wasMaster       was failed connection master
     * @param queryException  internal error
     * @param reconnected     connection status
     * @throws SQLException error with failover information
     */
    @Override
    public void throwFailoverMessage(HostAddress failHostAddress, boolean wasMaster, SQLException queryException,
                                     boolean reconnected) throws SQLException {
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

        String message;
        String sqlState;
        int vendorCode = 0;
        Throwable cause = null;

        if (queryException == null) {
            message = firstPart + error;
            sqlState = CONNECTION_EXCEPTION.getSqlState();
        } else {
            message = firstPart + queryException.getMessage() + ". " + error;
            sqlState = queryException.getSQLState();
            vendorCode = queryException.getErrorCode();
            cause = queryException.getCause();
        }

        if (reconnected && sqlState.startsWith("08")) {
            //change sqlState to "Transaction has been rolled back", to transaction exception, since reconnection has succeed
            sqlState = "25S03";
        }

        throw new SQLException(message, sqlState, vendorCode, cause);

    }

    public boolean canRetryFailLoop() {
        return currentConnectionAttempts.get() < urlParser.getOptions().failoverLoopRetries;
    }

    public abstract void reconnect() throws SQLException;

    public abstract boolean checkMasterStatus(SearchFilter searchFilter);

    public long getLastQueryNanos() {
        return lastQueryNanos;
    }

    protected boolean pingMasterProtocol(Protocol protocol) {
        try {
            protocol.ping();
            return true;
        } catch (SQLException e) {
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
     *
     * @param protocol connection to close.
     */
    public void closeConnection(Protocol protocol) {
        if (protocol != null && protocol.isConnected()) {
            protocol.close();
        }
    }

}
