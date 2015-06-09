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

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.query.MySQLQuery;
import org.mariadb.jdbc.internal.common.query.Query;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * this class handle the operation when multiple hosts.
 */
public class MultiHostListener implements FailoverListener {
    private final static Logger log = Logger.getLogger(MultiHostListener.class.getName());

    protected FailoverProxy proxy;
    protected MultiNodesProtocol masterProtocol;
    protected MultiNodesProtocol secondaryProtocol;
    ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
    protected AtomicBoolean isLooping = new AtomicBoolean();
    protected AtomicBoolean isLoopingMaster = new AtomicBoolean();
    protected AtomicBoolean isLoopingSecondary = new AtomicBoolean();
    protected long lastQueryTime = 0;
    protected ScheduledFuture scheduledPing = null;
    protected ScheduledFuture scheduledFailover = null;


    public MultiHostListener() {
        masterProtocol = null;
        secondaryProtocol = null;
    }

    public void setProxy(FailoverProxy proxy) {
        this.proxy = proxy;
    }


    public void initializeConnection() throws QueryException, SQLException {
        this.masterProtocol = (MultiNodesProtocol)this.proxy.currentProtocol;
        if (proxy.validConnectionTimeout != 0) scheduledPing = exec.schedule(new PingLoop(this), proxy.validConnectionTimeout, TimeUnit.SECONDS);
        try {
            launchSearchLoopConnection(true, true);
        } catch (QueryException e) {
            if (!this.masterProtocol.isConnected())proxy.masterHostFailTimestamp = System.currentTimeMillis();
            if (!this.secondaryProtocol.isConnected())proxy.secondaryHostFailTimestamp = System.currentTimeMillis();
            throw e;
        } catch (SQLException e) {
            if (!this.masterProtocol.isConnected())proxy.masterHostFailTimestamp = System.currentTimeMillis();
            if (!this.secondaryProtocol.isConnected())proxy.secondaryHostFailTimestamp = System.currentTimeMillis();
            throw e;
        }
    }


    public void postClose()  throws SQLException {
        if (!this.masterProtocol.isClosed()) this.masterProtocol.close();
        if (!this.secondaryProtocol.isClosed()) this.secondaryProtocol.close();
    }

    @Override
    public void preExecute() throws SQLException {
        if (shouldReconnect()) {
            launchAsyncSearchLoopConnection();
        } else if (proxy.validConnectionTimeout != 0) {
            lastQueryTime = System.currentTimeMillis();
            scheduledPing.cancel(true);
            scheduledPing = exec.schedule(new PingLoop(this), proxy.validConnectionTimeout, TimeUnit.SECONDS);
        }
    }



    /**
     * verify the different case when the connector must reconnect to host.
     * the autoreconnect parameter for multihost is only used after an error to try to reconnect and relaunched the operation silently.
     * So he doesn't appear here.
     * @return true if should reconnect.
     */
    private boolean shouldReconnect() {
        if (proxy.masterHostFailTimestamp !=0 || proxy.secondaryHostFailTimestamp !=0) {
            if (proxy.currentProtocol.inTransaction()) return false;
            if (proxy.currentConnectionAttempts > proxy.retriesAllDown) return false;
            long now = System.currentTimeMillis();

            if (proxy.masterHostFailTimestamp != 0) {
                if (proxy.queriesSinceFailover >= proxy.queriesBeforeRetryMaster) return true;
                if ((now - proxy.masterHostFailTimestamp) >= proxy.secondsBeforeRetryMaster * 1000) return true;
            }

            if (proxy.secondaryHostFailTimestamp != 0) {
                if ((now - proxy.secondaryHostFailTimestamp) >= proxy.secondsBeforeRetryMaster * 1000) return true;
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
                hostListener.launchSearchLoopConnection();
            } catch (Exception e) { }
            }
        });
    }

    /**
     * Loop to replace failed connections with valid ones.
     * @throws QueryException
     * @throws SQLException
     */
    public void launchSearchLoopConnection() throws QueryException, SQLException {
        proxy.currentConnectionAttempts++;
        proxy.lastRetry = System.currentTimeMillis();

        if (proxy.currentConnectionAttempts >= proxy.retriesAllDown) {
            throw new QueryException("Too many reconnection attempts ("+proxy.retriesAllDown+")");
        }

        boolean searchForMaster = (proxy.masterHostFailTimestamp > 0);
        boolean searchForSecondary = (proxy.secondaryHostFailTimestamp > 0);
        launchSearchLoopConnection(searchForMaster, searchForSecondary);
    }

    private void launchSearchLoopConnection(boolean searchForMaster, boolean searchForSecondary) throws QueryException, SQLException {
        log.fine("launchSearchLoopConnection searchForMaster:"+searchForMaster+ " loop:"+isLoopingMaster.get()+ "  secondary:"+searchForSecondary+" loop:"+isLoopingSecondary.get());
        QueryException queryException = null;
        SQLException sqlException = null;
        if (searchForMaster && isLoopingMaster.compareAndSet(false, true)) {
            try {
                MultiNodesProtocol newProtocol = new MultiNodesProtocol(this.masterProtocol.jdbcUrl,
                        this.masterProtocol.getUsername(),
                        this.masterProtocol.getPassword(),
                        this.masterProtocol.getInfo());
                newProtocol.setProxy(proxy);
                newProtocol.connectMaster(this);
            } catch (QueryException e1) {
                log.finest("launchSearchLoopConnection : master not found");
                queryException = e1;
            } catch (SQLException e1) {
                log.finest("launchSearchLoopConnection : master not found");
                sqlException = e1;
            } finally {
                isLoopingMaster.set(false);
            }
        }

        if (searchForSecondary && isLoopingSecondary.compareAndSet(false, true)) {
            try {
                MultiNodesProtocol newProtocol = new MultiNodesProtocol(this.masterProtocol.jdbcUrl,
                        this.masterProtocol.getUsername(),
                        this.masterProtocol.getPassword(),
                        this.masterProtocol.getInfo());
                newProtocol.setProxy(proxy);
                newProtocol.connectSecondary(this);
            } catch (QueryException e1) {
                log.finest("launchSearchLoopConnection : secondary not found");
                if (queryException == null) queryException = e1;
            } catch (SQLException e1) {
                if (sqlException == null) sqlException = e1;
            } finally {
                isLoopingSecondary.set(false);
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
        log.fine("found active master connection ");
        log.finest("proxy.currentReadOnlyAsked.get() : "+proxy.currentReadOnlyAsked.get());
        this.masterProtocol = newMasterProtocol;
        if (!proxy.currentReadOnlyAsked.get()) {
            //actually on a secondary read-only because master was unknown.
            //So select master as currentConnection
            try {
                syncConnection(proxy.currentProtocol, this.masterProtocol);
            } catch (Exception e) {
                log.fine("Some error append during connection parameter synchronisation : " + e.getMessage());
            }
            log.finest("switching current connection to master connection");
            proxy.currentProtocol = this.masterProtocol;
        }
        if (log.isLoggable(Level.INFO)) {
            if (proxy.masterHostFailTimestamp > 0) {
                log.info("new primary node [" + newMasterProtocol.currentHost.toString() + "] connection established after " + (System.currentTimeMillis() - proxy.masterHostFailTimestamp));
            } else log.info("new primary node [" + newMasterProtocol.currentHost.toString() + "] connection established");
        }
        proxy.resetMasterFailoverData();
    }


    /**
     * method called when a new secondary connection is found after a fallback
     * @param newSecondaryProtocol the new active connection
     */
    public synchronized void foundActiveSecondary(MultiNodesProtocol newSecondaryProtocol) {
        log.fine("found active secondary connection");
        this.secondaryProtocol = newSecondaryProtocol;

        //if asked to be on read only connection, switching to this new connection
        if (proxy.currentReadOnlyAsked.get() || (!proxy.currentReadOnlyAsked.get() && proxy.masterHostFailTimestamp > 0)) {
            try {
                syncConnection(proxy.currentProtocol, this.secondaryProtocol);
            } catch (Exception e) {
                log.fine("Some error append during connection parameter synchronisation : " + e.getMessage());
            }
            log.finest("switching current connection to secondary connection");
            proxy.currentProtocol = this.secondaryProtocol;
        }

        if (log.isLoggable(Level.INFO)) {
            if (proxy.secondaryHostFailTimestamp > 0) {
                log.info("new active secondary node [" + newSecondaryProtocol.currentHost.toString() + "] connection established after " + (System.currentTimeMillis() - proxy.secondaryHostFailTimestamp));
            } else log.info("new active secondary node [" + newSecondaryProtocol.currentHost.toString() + "] connection established");

        }
        proxy.resetSecondaryFailoverData();
    }

    /**
     * switch to a read-only(secondary) or read and write connection(master)
     * @param mustBeReadOnly the
     * @throws QueryException
     * @throws SQLException
     */
    @Override
    public void switchReadOnlyConnection(Boolean mustBeReadOnly) throws QueryException, SQLException {
        log.finest("switching to mustBeRealOnly = " + mustBeReadOnly + " mode");

        if (mustBeReadOnly != proxy.currentReadOnlyAsked.get() && proxy.currentProtocol.inTransaction()) {
            throw new QueryException("Trying to set to read-only mode during a transaction");
        }

        proxy.currentReadOnlyAsked.set(mustBeReadOnly);
        if (proxy.currentReadOnlyAsked.get()) {
            if (proxy.currentProtocol.isMasterConnection()) {
                //must change to replica connection
                if (proxy.secondaryHostFailTimestamp == 0) {
                    synchronized (this) {
                        log.finest("switching to secondary connection");
                        syncConnection(this.masterProtocol, this.secondaryProtocol);
                        proxy.currentProtocol = this.secondaryProtocol;
                    }
                }
            }
        } else {
            if (!proxy.currentProtocol.isMasterConnection()) {
                //must change to master connection
                if (proxy.masterHostFailTimestamp == 0) {
                    synchronized (this) {
                        log.finest("switching to master connection");
                        syncConnection(this.secondaryProtocol, this.masterProtocol);
                        proxy.currentProtocol = this.masterProtocol;
                    }
                } else {
                    if (proxy.autoReconnect) {
                        try {
                            launchSearchLoopConnection();
                            //connection established, no need to send Exception !
                            return;
                        } catch (Exception e) { }
                    }

                    if (isLooping.compareAndSet(false, true)) {
                        scheduledFailover = exec.scheduleWithFixedDelay(new FailLoop(this), 250, 250, TimeUnit.MILLISECONDS);
                    }
                    throw new QueryException("No primary host is actually connected");
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
            if(this.masterProtocol.ping()) {
                log.finest("SQL Primary node [" + this.masterProtocol.currentHost.toString() + "] connection re-established");
                return relaunchOperation(method, args);
            }
        } catch (Exception e) { }

        if (proxy.autoReconnect && !this.masterProtocol.inTransaction()) {
            try {
                launchSearchLoopConnection();
                log.finest("SQL Primary node [" + this.masterProtocol.currentHost.toString() + "] connection re-established");

                //now that we are reconnect, relaunched result if the result was not crashing the node
                return relaunchOperation(method, args);
            } catch (Exception e) { }
        }

        //in multiHost, switch to secondary if active
        if (proxy.secondaryHostFailTimestamp == 0) {
            try {
                if(this.secondaryProtocol.ping()) {
                    log.finest("switching to secondary connection");
                    syncConnection(this.masterProtocol, this.secondaryProtocol);
                    proxy.currentProtocol = this.secondaryProtocol;
                    launchReConnectingTimerLoop(true);
                    return relaunchOperation(method, args);
                }
            } catch (Exception e) { }
        }

        log.finest("no secondary failover active");
        launchReConnectingTimerLoop(true);
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

        if (proxy.masterHostFailTimestamp == 0) {

            try {
                this.masterProtocol.ping(); //check that master is on before switching to him
                log.finest("switching to master connection");
                syncConnection(this.secondaryProtocol, this.masterProtocol);
                proxy.currentProtocol = this.masterProtocol;

                launchReConnectingTimerLoop(true); //launch reconnection loop
                return relaunchOperation(method, args); //now that we are on master, relaunched result if the result was not crashing the master

            } catch (Exception e) {
                if (proxy.masterHostFailTimestamp == 0) proxy.masterHostFailTimestamp = System.currentTimeMillis();
            }
        }

        if (proxy.autoReconnect) {
            try {
                launchSearchLoopConnection();
                log.finest("SQL Primary node [" + this.masterProtocol.currentHost.toString() + "] connection re-established");
                return relaunchOperation(method, args); //now that we are reconnect, relaunched result if the result was not crashing the node
            } catch (Exception ee) {
                //in case master is down and another slave has been found
                if (proxy.secondaryHostFailTimestamp == 0) {
                    return relaunchOperation(method, args);
                }
                launchReConnectingTimerLoop(false);
                return new HandleErrorResult();
            }
        }
        launchReConnectingTimerLoop(true);
        return new HandleErrorResult();
    }


    /**
     * launch the scheduler loop every 250 milliseconds, to reconnect a failed connection.
     * Will verify if there is an existing scheduler
     * @param now now will launch the loop immediatly, 250ms after if false
     */
    protected void launchReConnectingTimerLoop(boolean now) {
        if (isLooping.compareAndSet(false, true)) {
            scheduledFailover = exec.scheduleWithFixedDelay(new FailLoop(this), now?0:250, 250, TimeUnit.MILLISECONDS);
        }
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
                    handleErrorResult.resultObject = method.invoke(proxy.currentProtocol, args);
                    handleErrorResult.mustThrowError = false;
                }
            } else {
                handleErrorResult.resultObject = method.invoke(proxy.currentProtocol, args);
                handleErrorResult.mustThrowError = false;
            }
        }
        return handleErrorResult;
    }


    protected void stopFailover() {
        scheduledFailover.cancel(false);
        isLooping.set(false);
    }

    /**
     * private class to permit a timer reconnection loop
     */
    protected class FailLoop implements Runnable {
        MultiHostListener listener;
        public FailLoop(MultiHostListener listener) {
            log.finest("launched search loop");
            this.listener = listener;
        }

        public void run() {
            log.finest("FailLoop run master:"+(proxy.masterHostFailTimestamp != 0)+" slave:"+(proxy.secondaryHostFailTimestamp != 0));
            if (proxy.masterHostFailTimestamp != 0 || proxy.secondaryHostFailTimestamp != 0) {
                if (listener.shouldReconnect()) {
                    log.finest("--2 run master:"+(proxy.masterHostFailTimestamp != 0)+" slave:"+(proxy.secondaryHostFailTimestamp != 0));
                    try {
                        log.finest("--3 run master:"+(proxy.masterHostFailTimestamp != 0)+" slave:"+(proxy.secondaryHostFailTimestamp != 0));
                        listener.launchSearchLoopConnection();
                        log.finest("--4 run master:"+(proxy.masterHostFailTimestamp != 0)+" slave:"+(proxy.secondaryHostFailTimestamp != 0));
                        //reconnection done !
                        listener.stopFailover();
                    } catch (Exception e) {
                        log.finest("FailLoop search connection failed");
                        //do nothing
                    }
                } else {
                    if (proxy.currentConnectionAttempts > proxy.retriesAllDown) listener.stopFailover();
                }
            } else {
                listener.stopFailover();
            }
        }
    }

    /**
     * private class to chech of currents connections are still ok.
     */
    private class PingLoop implements Runnable {
        MultiHostListener listener;
        public PingLoop(MultiHostListener listener) {
            this.listener = listener;
        }

        public void run() {
            try {
                log.finest("PingLoop run");
                boolean masterFail = false;
                try {
                    if (masterProtocol.ping()) {
                        try {
                            if (!masterProtocol.checkIfMaster()) {
                                //the connection that was master isn't now
                                masterFail = true;
                            }
                        } catch (SQLException e) {
                            //do nothing
                        }
                    }
                } catch (QueryException e) {
                    masterFail = true;
                }

                if (masterFail) {
                    if (proxy.masterHostFailTimestamp == 0) proxy.masterHostFailTimestamp = System.currentTimeMillis();
                    proxy.currentConnectionAttempts = 0;
                    try {
                        listener.primaryFail(null, null);
                    } catch (Throwable t) {
                        //do nothing
                    }
                }
            } finally {
                // Reschedule
                exec.schedule(this, proxy.validConnectionTimeout, TimeUnit.SECONDS);
            }
        }
    }
}
