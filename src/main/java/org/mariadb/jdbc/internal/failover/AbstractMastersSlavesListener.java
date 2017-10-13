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

package org.mariadb.jdbc.internal.failover;

import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.failover.tools.SearchFilter;
import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.pool.GlobalStateInfo;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


public abstract class AbstractMastersSlavesListener extends AbstractMastersListener {


    private static final Logger logger = LoggerFactory.getLogger(AbstractMastersSlavesListener.class);
    //These reference are when failloop reconnect failing connection, but lock is already held by
    //another thread (query in progress), so switching the connection wait for the query to be finish.
    //next query will reconnect those during preExecute method, or if actual used connection failed
    //during reconnection phase.
    protected final AtomicReference<Protocol> waitNewSecondaryProtocol = new AtomicReference<Protocol>();
    protected final AtomicReference<Protocol> waitNewMasterProtocol = new AtomicReference<Protocol>();
    /* =========================== Failover variables ========================================= */
    private volatile long secondaryHostFailNanos = 0;
    private final AtomicBoolean secondaryHostFail = new AtomicBoolean();

    protected AbstractMastersSlavesListener(UrlParser urlParser, final GlobalStateInfo globalInfo) {
        super(urlParser, globalInfo);
        this.secondaryHostFail.set(true);
    }

    /**
     * Handle failover on master or slave connection.
     *
     * @param method   called method
     * @param args     methods parameters
     * @param protocol current protocol
     * @return HandleErrorResult object to indicate if query has finally been relaunched or exception if not.
     * @throws Throwable if method with parameters doesn't exist
     */
    public HandleErrorResult handleFailover(SQLException qe, Method method, Object[] args, Protocol protocol) throws Throwable {
        if (isExplicitClosed()) {
            throw new SQLException("Connection has been closed !");
        }

        //check that failover is due to kill command
        boolean killCmd = qe != null
                && qe.getSQLState() != null
                && qe.getSQLState().equals("70100")
                && 1927 == qe.getErrorCode();

        if (protocol.mustBeMasterConnection()) {
            if (!protocol.isMasterConnection()) {
                logger.warn("SQL Primary node [{}, conn={}] is now in read-only mode. Exception : {}",
                        this.currentProtocol.getHostAddress().toString(),
                        this.currentProtocol.getServerThreadId(),
                        qe.getMessage());
            } else if (setMasterHostFail()) {
                logger.warn("SQL Primary node [{}, conn={}] connection fail. Reason : {}",
                        this.currentProtocol.getHostAddress().toString(),
                        this.currentProtocol.getServerThreadId(),
                        qe.getMessage());

                addToBlacklist(protocol.getHostAddress());
            }
            return primaryFail(method, args, killCmd);
        } else {
            if (setSecondaryHostFail()) {
                logger.warn("SQL secondary node [{}, conn={}] connection fail. Reason : {}",
                        this.currentProtocol.getHostAddress().toString(),
                        this.currentProtocol.getServerThreadId(),
                        qe.getMessage());
                addToBlacklist(protocol.getHostAddress());
            }
            return secondaryFail(method, args, killCmd);
        }
    }

    @Override
    protected void resetMasterFailoverData() {
        super.resetMasterFailoverData();

        //if all connection are up, reset failovers timers
        if (!secondaryHostFail.get()) {
            currentConnectionAttempts.set(0);
            lastRetry = 0;
        }
    }

    protected void resetSecondaryFailoverData() {
        if (secondaryHostFail.compareAndSet(true, false)) {
            secondaryHostFailNanos = 0;
        }

        //if all connection are up, reset failovers timers
        if (!isMasterHostFail()) {
            currentConnectionAttempts.set(0);
            lastRetry = 0;
        }
    }

    public long getSecondaryHostFailNanos() {
        return secondaryHostFailNanos;
    }

    /**
     * Set slave connection lost variables.
     *
     * @return true if fail wasn't seen before
     */
    public boolean setSecondaryHostFail() {
        if (secondaryHostFail.compareAndSet(false, true)) {
            secondaryHostFailNanos = System.nanoTime();
            currentConnectionAttempts.set(0);
            return true;
        }
        return false;
    }

    public boolean isSecondaryHostFail() {
        return secondaryHostFail.get();
    }

    public boolean isSecondaryHostFailReconnect() {
        return secondaryHostFail.get() && waitNewSecondaryProtocol.get() == null;
    }

    public boolean isMasterHostFailReconnect() {
        return isMasterHostFail() && waitNewMasterProtocol.get() == null;
    }


    public boolean hasHostFail() {
        return isSecondaryHostFailReconnect() || isMasterHostFailReconnect();
    }

    public SearchFilter getFilterForFailedHost() {
        return new SearchFilter(isMasterHostFail(), isSecondaryHostFail());
    }

    public abstract HandleErrorResult secondaryFail(Method method, Object[] args, boolean killCmd) throws Throwable;

    public abstract void foundActiveSecondary(Protocol newSecondaryProtocol) throws SQLException;

}
