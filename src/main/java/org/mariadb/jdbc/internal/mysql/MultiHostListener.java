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

package org.mariadb.jdbc.internal.mysql;

import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.query.MySQLQuery;
import org.mariadb.jdbc.internal.common.query.Query;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * this class handle the operation when multiple hosts.
 */
public class MultiHostListener extends BaseFailoverListener implements FailoverListener{
    private final static Logger log = Logger.getLogger(MultiHostListener.class.getName());

    protected MultiNodesProtocol masterProtocol;
    protected MultiNodesProtocol secondaryProtocol;
    protected long lastQueryTime = 0;
    protected ScheduledFuture scheduledPing = null;

    public MultiHostListener() {
        masterProtocol = null;
        secondaryProtocol = null;
    }

    public void initializeConnection(Protocol protocol) throws QueryException, SQLException {
        this.masterProtocol = (MultiNodesProtocol)protocol;
        this.currentProtocol = this.masterProtocol;
        parseHAOptions(protocol);
        //if (validConnectionTimeout != 0) scheduledPing = Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new PingLoop(this), 1, 1, TimeUnit.SECONDS);

        if (validConnectionTimeout != 0) scheduledPing = Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new PingLoop(this), validConnectionTimeout, validConnectionTimeout, TimeUnit.SECONDS);
        try {
            reconnectFailedConnection(true, true, true);
        } catch (QueryException e) {
            checkInitialConnection();
            throw e;
        } catch (SQLException e) {
            checkInitialConnection();
            throw e;
        }
    }

    protected void checkInitialConnection() {
        if (this.masterProtocol != null && !this.masterProtocol.isConnected()) {
            log.warning("masterHostFail must not BE here !!!!!!!!!!!!!!!! ");
            setMasterHostFail();
        }
        if (this.secondaryProtocol != null && !this.secondaryProtocol.isConnected()) {
            log.warning("secondaryHostFail must not BE here !!!!!!!!!!!!!!!! ");
            setSecondaryHostFail();
        }
        launchFailLoopIfNotlaunched(false);
    }

    public void postClose()  throws SQLException {
        if (scheduledPing != null) scheduledPing.cancel(true);
        stopFailover();
        if (!this.masterProtocol.isClosed()) this.masterProtocol.close();
        if (!this.secondaryProtocol.isClosed()) this.secondaryProtocol.close();
    }

    @Override
    public void preExecute() throws SQLException {
        if (isMasterHostFail())queriesSinceFailover++;

        if (shouldReconnect()) {
            launchAsyncSearchLoopConnection();
        } else if (validConnectionTimeout != 0) {
            lastQueryTime = System.currentTimeMillis();
            scheduledPing.cancel(true);
            scheduledPing = Executors.newSingleThreadScheduledExecutor().schedule(new PingLoop(this), validConnectionTimeout, TimeUnit.SECONDS);
        }
    }



    /**
     * verify the different case when the connector must reconnect to host.
     * the autoreconnect parameter for multihost is only used after an error to try to reconnect and relaunched the operation silently.
     * So he doesn't appear here.
     * @return true if should reconnect.
     */
    public boolean shouldReconnect() {
        if (isMasterHostFail() || isSecondaryHostFail()) {
            if (currentProtocol.inTransaction()) return false;
            if (currentConnectionAttempts > retriesAllDown) return false;
            long now = System.currentTimeMillis();

            if (isMasterHostFail()) {
                if (queriesSinceFailover >= queriesBeforeRetryMaster) return true;
                if ((now - getMasterHostFailTimestamp()) >= secondsBeforeRetryMaster * 1000) return true;
            }

            if (isSecondaryHostFail()) {
                if ((now - getSecondaryHostFailTimestamp()) >= secondsBeforeRetryMaster * 1000) return true;
            }
        }
        return false;
    }

    /**
     * Asynchronous Loop to replace failed connections with valid ones.
     *
     */
    public void launchAsyncSearchLoopConnection() {
        final MultiHostListener hostListener = MultiHostListener.this;
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
            try {
                hostListener.reconnectFailedConnection();
            } catch (Exception e) { }
            }
        });
    }

    /**
     * Loop to replace failed connections with valid ones.
     * @throws QueryException
     * @throws SQLException
     */
    public void reconnectFailedConnection() throws QueryException, SQLException {
        currentConnectionAttempts++;
        lastRetry = System.currentTimeMillis();

        if (currentConnectionAttempts >= retriesAllDown) {
            throw new QueryException("Too many reconnection attempts ("+retriesAllDown+")");
        }
        reconnectFailedConnection(isMasterHostFail(), isSecondaryHostFail(), false);
    }

    public synchronized void reconnectFailedConnection(boolean searchForMaster, boolean searchForSecondary, boolean initialConnection) throws QueryException, SQLException {
        QueryException queryException = null;
        SQLException sqlException = null;
        if (searchForMaster && (isMasterHostFail() || initialConnection)) {
            try {
                MultiNodesProtocol newProtocol = new MultiNodesProtocol(this.masterProtocol.jdbcUrl);
                newProtocol.setProxy(proxy);
                newProtocol.connectMaster(this);
            } catch (QueryException e1) {
                log.finest("reconnectFailedConnection : master not found");
                queryException = e1;
            }
        }

        if (searchForSecondary && (isSecondaryHostFail()|| initialConnection)) {
            try {
                MultiNodesProtocol newProtocol = new MultiNodesProtocol(this.masterProtocol.jdbcUrl);
                newProtocol.setProxy(proxy);
                newProtocol.connectSecondary(this);
            } catch (QueryException e1) {
                log.finest("launchSearchLoopConnection : secondary not found");
                if (queryException == null) queryException = e1;
            }
        }
        if (sqlException != null )  throw sqlException;
        if (queryException != null )  throw queryException;
    }

    /**
     * method called when a new Master connection is found after a fallback
     * @param newMasterProtocol the new active connection
     */
    public synchronized void foundActiveMaster(MultiNodesProtocol newMasterProtocol) {
        log.fine("found active master connection "+newMasterProtocol.getHostAddress());
        log.finest("currentReadOnlyAsked.get() : " + currentReadOnlyAsked.get());
        this.masterProtocol = newMasterProtocol;
        if (!currentReadOnlyAsked.get()) {
            //actually on a secondary read-only because master was unknown.
            //So select master as currentConnection
            try {
                syncConnection(currentProtocol, this.masterProtocol);
            } catch (Exception e) {
                log.fine("Some error append during connection parameter synchronisation : " + e.getMessage());
            }
            log.finest("switching current connection to master connection");
            currentProtocol = this.masterProtocol;
        }
        if (log.isLoggable(Level.INFO)) {
            if (isMasterHostFail()) {
                log.info("new primary node [" + newMasterProtocol.currentHost.toString() + "] connection established after " + (System.currentTimeMillis() - getMasterHostFailTimestamp()));
            } else log.info("new primary node [" + newMasterProtocol.currentHost.toString() + "] connection established");
        }
        resetMasterFailoverData();

    }


    /**
     * method called when a new secondary connection is found after a fallback
     * @param newSecondaryProtocol the new active connection
     */
    public synchronized void foundActiveSecondary(MultiNodesProtocol newSecondaryProtocol) {
        log.fine("found active secondary connection");
        this.secondaryProtocol = newSecondaryProtocol;

        //if asked to be on read only connection, switching to this new connection
        if (currentReadOnlyAsked.get() || (!currentReadOnlyAsked.get() && isMasterHostFail())) {
            try {
                syncConnection(currentProtocol, this.secondaryProtocol);
            } catch (Exception e) {
                log.fine("Some error append during connection parameter synchronisation : " + e.getMessage());
            }
            log.finest("switching current connection to secondary connection");
            currentProtocol = this.secondaryProtocol;
        }

        if (log.isLoggable(Level.INFO)) {
            if (isSecondaryHostFail()) {
                log.info("new active secondary node [" + newSecondaryProtocol.currentHost.toString() + "] connection established after " + (System.currentTimeMillis() - getSecondaryHostFailTimestamp()));
            } else log.info("new active secondary node [" + newSecondaryProtocol.currentHost.toString() + "] connection established");

        }
        resetSecondaryFailoverData();
    }

    /**
     * switch to a read-only(secondary) or read and write connection(master)
     * @param mustBeReadOnly the
     * @throws QueryException
     * @throws SQLException
     */
    @Override
    public void switchReadOnlyConnection(Boolean mustBeReadOnly) throws QueryException, SQLException {
        log.finest("switching to mustBeReadOnly = " + mustBeReadOnly + " mode");

        if (mustBeReadOnly != currentReadOnlyAsked.get() && currentProtocol.inTransaction()) {
            throw new QueryException("Trying to set to read-only mode during a transaction");
        }
        if (currentReadOnlyAsked.compareAndSet(!mustBeReadOnly, mustBeReadOnly)) {
            if (currentReadOnlyAsked.get()) {
                if (currentProtocol.isMasterConnection()) {
                    //must change to replica connection
                    if (!isSecondaryHostFail()) {
                        synchronized (this) {
                            log.finest("switching to secondary connection");
                            syncConnection(this.masterProtocol, this.secondaryProtocol);
                            currentProtocol = this.secondaryProtocol;
                        }
                    }
                }
            } else {
                if (!currentProtocol.isMasterConnection()) {
                    //must change to master connection
                    if (!isMasterHostFail()) {
                        synchronized (this) {
                            log.finest("switching to master connection");
                            syncConnection(this.secondaryProtocol, this.masterProtocol);
                            currentProtocol = this.masterProtocol;
                        }
                    } else {
                        if (autoReconnect) {
                            try {
                                reconnectFailedConnection();
                                //connection established, no need to send Exception !
                                return;
                            } catch (Exception e) { }
                        }
                        launchFailLoopIfNotlaunched(false);
                        throw new QueryException("No primary host is actually connected");
                    }
                }
            }
            
        }
    }

    /**
     * when switching between 2 connections, report existing connection parameter to the new used connection
     * @param from used connection
     * @param to will-be-current connection
     * @throws QueryException
     * @throws SQLException
     */
    protected void syncConnection(Protocol from, Protocol to) throws QueryException, SQLException {
        to.setMaxAllowedPacket(from.getMaxAllowedPacket());
        to.setMaxRows(from.getMaxRows());
        to.setInternalMaxRows(from.getMaxRows());
        if (from.getTransactionIsolationLevel() != 0) {
            to.setTransactionIsolation(from.getTransactionIsolationLevel());
        }
        try {
            if (from.getDatabase() != null && !"".equals(from.getDatabase())) {
                    to.selectDB(from.getDatabase());
            }
            if (from.getAutocommit() != to.getAutocommit()) {
                to.executeQuery(new MySQLQuery("set autocommit=" + (from.getAutocommit()?"1":"0")));
            }
        } catch (QueryException e) {
            e.printStackTrace();
        }
    }

    /**
     * to handle the newly detected failover on the master connection
     * @param method
     * @param args
     * @return an object to indicate if the previous Exception must be thrown, or the object resulting if a failover worked
     * @throws Throwable
     */
    public synchronized HandleErrorResult primaryFail(Method method, Object[] args) throws Throwable {
        log.warning("SQL Primary node [" + this.masterProtocol.currentHost.toString() + "] connection fail ");

        //try to reconnect automatically only time before looping
        try {
            if(this.masterProtocol != null && this.masterProtocol.ping()) {
                log.finest("SQL Primary node [" + this.masterProtocol.currentHost.toString() + "] connection re-established");
                return relaunchOperation(method, args);
            }
        } catch (Exception e) { }

        if (autoReconnect && !this.masterProtocol.inTransaction()) {
            try {
                reconnectFailedConnection();
                log.finest("SQL Primary node [" + this.masterProtocol.currentHost.toString() + "] connection re-established");

                //now that we are reconnect, relaunched result if the result was not crashing the node
                return relaunchOperation(method, args);
            } catch (Exception e) { }
        }

        //in multiHost, switch to secondary if active, even if in a current transaction -> will throw an exception
        if (!isSecondaryHostFail()) {
            try {
                if(this.secondaryProtocol != null && this.secondaryProtocol.ping()) {
                    log.finest("switching to secondary connection");
                    syncConnection(this.masterProtocol, this.secondaryProtocol);
                    currentProtocol = this.secondaryProtocol;
                    launchFailLoopIfNotlaunched(true);
                    try {
                        return relaunchOperation(method, args);
                    } catch (Exception e) {
                        //if a problem since ping, just launched the first exception
                    }
                } else log.finest("ping failed on secondary");
            } catch (Exception e) {
                setMasterHostFail();
                log.log(Level.FINEST, "ping on secondary failed",e);
            }
        } else log.finest("secondary is already down");

        log.finest("no secondary failover active");
        launchFailLoopIfNotlaunched(true);
        return new HandleErrorResult();
    }

    /**
     * to handle the newly detected failover on the secondary connection
     * @param method
     * @param args
     * @return an object to indicate if the previous Exception must be thrown, or the object resulting if a failover worked
     * @throws Throwable
     */
    public synchronized HandleErrorResult secondaryFail(Method method, Object[] args) throws Throwable {

        if (!isMasterHostFail()) {
            try {
                if (this.masterProtocol !=null) {
                    this.masterProtocol.ping(); //check that master is on before switching to him
                    log.finest("switching to master connection");
                    syncConnection(this.secondaryProtocol, this.masterProtocol);
                    currentProtocol = this.masterProtocol;

                    launchFailLoopIfNotlaunched(true); //launch reconnection loop
                    return relaunchOperation(method, args); //now that we are on master, relaunched result if the result was not crashing the master
                }
            } catch (Exception e) {
                setMasterHostFail();
            }
        }

        if (autoReconnect) {
            try {
                reconnectFailedConnection();
                log.finest("SQL Primary node [" + this.masterProtocol.currentHost.toString() + "] connection re-established");
                return relaunchOperation(method, args); //now that we are reconnect, relaunched result if the result was not crashing the node
            } catch (Exception ee) {
                //in case master is down and another slave has been found
                if (!isSecondaryHostFail()) {
                    return relaunchOperation(method, args);
                }
                launchFailLoopIfNotlaunched(false);
                return new HandleErrorResult();
            }
        }
        launchFailLoopIfNotlaunched(true);
        return new HandleErrorResult();
    }


    /**
     * After a failover that has bean done, relaunche the operation that was in progress.
     * In case of special operation that crash serveur, doesn't relaunched it;
     * @param method the methode accessed
     * @param args the parameters
     * @return An object that indicate the result or that the exception as to be thrown
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    protected HandleErrorResult relaunchOperation(Method method, Object[] args) throws IllegalAccessException, InvocationTargetException{
        HandleErrorResult handleErrorResult = new HandleErrorResult();
        if (method != null) {
            if ("executeQuery".equals(method.getName())) {
                String query = ((Query)args[0]).getQuery().toUpperCase();
                if (!query.equals("ALTER SYSTEM CRASH")
                        && !query.startsWith("KILL")) {
                    handleErrorResult.resultObject = method.invoke(currentProtocol, args);
                    handleErrorResult.mustThrowError = false;
                }
            } else {
                handleErrorResult.resultObject = method.invoke(currentProtocol, args);
                handleErrorResult.mustThrowError = false;
            }
        }
        return handleErrorResult;
    }




    /**
     * private class to chech of currents connections are still ok.
     */
    protected class PingLoop implements Runnable {
        MultiHostListener listener;
        public PingLoop(MultiHostListener listener) {
            this.listener = listener;
        }

        public void run() {
            //if (lastQueryTime + validConnectionTimeout < System.currentTimeMillis()) {
                log.finest("PingLoop run");
                boolean masterFail = false;
                try {
                    if (masterProtocol.ping()) {
                        if (!masterProtocol.checkIfMaster()) {
                            //the connection that was master isn't now
                            masterFail = true;
                        }
                    }
                } catch (QueryException e) {
                    masterFail = true;
                }

                if (masterFail) {
                    if (setMasterHostFail()) {
                        currentConnectionAttempts = 0;
                        try {
                            listener.primaryFail(null, null);
                        } catch (Throwable t) {
                            //do nothing
                        }
                    }
                }
            //}
        }
    }
}
