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
import org.mariadb.jdbc.internal.common.query.Query;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

    public MultiHostListener() {
        masterProtocol = null;
        secondaryProtocol = null;
    }

    public void setProxy(FailoverProxy proxy) {
        this.proxy = proxy;
    }


    public void initializeConnection() throws QueryException, SQLException {
        this.masterProtocol = (MultiNodesProtocol)this.proxy.currentProtocol;
        this.secondaryProtocol = new MultiNodesProtocol(this.masterProtocol.jdbcUrl,
                this.masterProtocol.getUsername(),
                this.masterProtocol.getPassword(),
                this.masterProtocol.getInfo());
        //set failover data to force connection
        proxy.masterHostFailTimestamp = System.currentTimeMillis();
        proxy.secondaryHostFailTimestamp = System.currentTimeMillis();
        //TODO for perf : initial masterPrococol load and replace by connected one -> can be better
        launchSearchLoopConnection();
    }


    public void postClose()  throws SQLException {
        if (!this.masterProtocol.isClosed()) this.masterProtocol.close();
        if (!this.secondaryProtocol.isClosed()) this.secondaryProtocol.close();
    }

    @Override
    public void preExecute() throws SQLException {
        if (shouldReconnect()) {
            launchAsyncSearchLoopConnection();
        }
    }

    /**
     * verify the different case when the connector must reconnect to host.
     * the autoreconnect parameter for multihost is only used after an error to try to reconnect and relaunched the operation silently.
     * So he doesn't appear here.
     * @return true if should reconnect.
     */
    private boolean shouldReconnect() {
        if (proxy.currentProtocol.inTransaction()) return false;
        if (proxy.currentConnectionAttempts > proxy.retriesAllDown) return false;
        long now = System.currentTimeMillis();

        if (proxy.masterHostFailTimestamp !=0) {
            if ((now - proxy.masterHostFailTimestamp) / 1000 > proxy.secondsBeforeRetryMaster) return true;
            if (proxy.queriesSinceFailover > proxy.queriesBeforeRetryMaster) return true;
        }

        if (proxy.secondaryHostFailTimestamp !=0) {
            if ((now - proxy.secondaryHostFailTimestamp) / 1000 > proxy.secondsBeforeRetryMaster) return true;
            if (proxy.queriesSinceFailover > proxy.queriesBeforeRetryMaster) return true;
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
        MultiNodesProtocol newProtocol = new MultiNodesProtocol(this.masterProtocol.jdbcUrl,
                this.masterProtocol.getUsername(),
                this.masterProtocol.getPassword(),
                this.masterProtocol.getInfo());
        proxy.currentConnectionAttempts++;
        proxy.lastRetry = System.currentTimeMillis();

        if (proxy.currentConnectionAttempts >= proxy.retriesAllDown) {
            throw new QueryException("Too many reconnection attempts ("+proxy.retriesAllDown+")");
        }

        boolean searchForMaster = (proxy.masterHostFailTimestamp > 0);
        boolean searchForSecondary = (proxy.secondaryHostFailTimestamp > 0);

        List<HostAddress> loopSecondaryAddresses = new LinkedList<HostAddress>(Arrays.asList(this.masterProtocol.jdbcUrl.getHostAddresses().clone()));
        loopSecondaryAddresses.remove(this.masterProtocol.jdbcUrl.getHostAddresses()[0]);

        QueryException queryException = null;
        SQLException sqlException = null;
        if (searchForMaster && isLoopingMaster.compareAndSet(false, true)) {
            try {
                newProtocol.connectMaster(this);
                isLoopingMaster.set(false);
            } catch (QueryException e1) {
                queryException = e1;
            } catch (SQLException e1) {
                sqlException = e1;
            }
        }

        if (searchForSecondary && isLoopingSecondary.compareAndSet(false, true)) {
            try {
                newProtocol.connectSecondary(this, loopSecondaryAddresses);
                isLoopingSecondary.set(false);
            } catch (QueryException e1) {
                if (queryException == null) queryException = e1;
            } catch (SQLException e1) {
                if (sqlException == null) sqlException = e1;
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
        log.fine("found active master connection");
        this.masterProtocol = newMasterProtocol;
        if (!proxy.currentReadOnlyAsked.get()) {
            //actually on a secondary read-only because master was unknown.
            //So select master as currentConnection
            try {
                syncConnection(proxy.currentProtocol, this.masterProtocol);
            } catch (Exception e) {
                log.fine("Some error append during connection parameter synchronisation : " + e.getMessage());
            }
            log.finest("switching to master connection");
            proxy.currentProtocol = this.masterProtocol;
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
        if (proxy.currentReadOnlyAsked.get() && proxy.currentProtocol.isMasterConnection()) {
            //actually on a master since the failover, so switching to a read-only connection as asked
            try {
                syncConnection(proxy.currentProtocol, this.secondaryProtocol);
            } catch (Exception e) {
                log.fine("Some error append during connection parameter synchronisation : " + e.getMessage());
            }
            log.finest("switching to secondary connection");
            proxy.currentProtocol = this.secondaryProtocol;
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
                        exec.scheduleAtFixedRate(new failLoop(this), 250, 250, TimeUnit.MILLISECONDS);
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
    private void syncConnection(Protocol from, Protocol to) throws QueryException, SQLException {
        to.setMaxAllowedPacket(from.getMaxAllowedPacket());
        to.setMaxRows(from.getMaxRows());
        if (from.getDatabase() != null && !"".equals(from.getDatabase())) {
            to.selectDB(to.getDatabase());
        }

        //TODO check if must handle transaction && autocommit ?

    }

    /**
     * to handle the newly detected failover on the master connection
     * @param method
     * @param args
     * @return an object to indicate if the previous Exception must be thrown, or the object resulting if a failover worked
     * @throws Throwable
     */
    public synchronized HandleErrorResult primaryFail(Method method, Object[] args) throws Throwable {
        log.warning("SQL Primary node [" + this.masterProtocol.currentHost + "] connection fail");
        HandleErrorResult handleErrorResult = new HandleErrorResult();
        if (proxy.autoReconnect) {
            try {
                launchSearchLoopConnection();
                //connection established !

                //now that we are reconnect, relaunched result if the result was not crashing the node
                return relaunchOperation(method, args);
            } catch (Exception e) {
                if (isLooping.compareAndSet(false, true)) {
                    exec.scheduleAtFixedRate(new failLoop(this), 250, 250, TimeUnit.MILLISECONDS);
                }
            }
        }

        //in multiHost, switch to secondary
        log.finest("switching to secondary connection");
        syncConnection(this.masterProtocol, this.secondaryProtocol);
        proxy.currentProtocol = this.secondaryProtocol;

        //loop reconnection if not already launched
        if (isLooping.compareAndSet(false, true)) {
            exec.scheduleAtFixedRate(new failLoop(this), 0, 250, TimeUnit.MILLISECONDS);
        }
        return handleErrorResult;
    }

    /**
     * to handle the newly detected failover on the secondary connection
     * @param method
     * @param args
     * @return an object to indicate if the previous Exception must be thrown, or the object resulting if a failover worked
     * @throws Throwable
     */
    public synchronized HandleErrorResult secondaryFail(Method method, Object[] args) throws Throwable {
        //in multiHost, switch temporary to Master
        log.finest("switching to master connection");
        syncConnection(this.secondaryProtocol, this.masterProtocol);
        proxy.currentProtocol = this.masterProtocol;

        //launch reconnection loop
        if (isLooping.compareAndSet(false, true)) {
            exec.scheduleAtFixedRate(new failLoop(this), 0, 250, TimeUnit.MILLISECONDS);
        }

        //now that we are on master, relaunched result if the result was not crashing the master
        return relaunchOperation(method, args);
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
    private HandleErrorResult relaunchOperation(Method method, Object[] args) throws IllegalAccessException, InvocationTargetException{
        HandleErrorResult handleErrorResult = new HandleErrorResult();
        if ("executeQuery".equals(method.getName())) {
            String query = ((Query)args[0]).getQuery().toUpperCase();
            if (!query.equals("ALTER SYSTEM CRASH")
                    && query.startsWith("KILL")) {
                handleErrorResult.resultObject = method.invoke(proxy.currentProtocol, args);
                handleErrorResult.mustThrowError = false;
            }
        } else {
            handleErrorResult.resultObject = method.invoke(proxy.currentProtocol, args);
            handleErrorResult.mustThrowError = false;
        }
        return handleErrorResult;
    }


    protected void stopFailover() {
        exec.shutdown();
        isLooping.set(false);
    }

    /**
     * private class to permit a timer reconnection loop
     */
    private class failLoop implements Runnable {
        MultiHostListener listener;
        public failLoop(MultiHostListener listener) {
            this.listener = listener;
        }

        public void run() {
            if (listener.shouldReconnect()) {
                try {
                    listener.launchSearchLoopConnection();
                    //reconnection done !
                    listener.stopFailover();
                } catch (Exception e) {
                    //do nothing
                }
            } else {
                if (proxy.currentConnectionAttempts > proxy.retriesAllDown) listener.stopFailover();
            }
        }
    }
}
