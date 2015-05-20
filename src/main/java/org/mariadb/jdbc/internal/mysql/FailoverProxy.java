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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class FailoverProxy implements InvocationHandler {
    private final static Logger log = Logger.getLogger(FailoverProxy.class.getName());


    /* =========================== Failover  parameters ========================================= */
    /**
     * Driver must recreateConnection after a failover
     */
    protected boolean autoReconnect = false;
    protected boolean autoReconnectForPools = false;

    /**
     * If autoReconnect is enabled, the initial time to wait between re-connect attempts (in seconds, defaults to 2)
     */
    protected int initialTimeout = 2;

    /**
     * Maximum number of reconnects to attempt if autoReconnect is true, default is 3
     */
    protected int maxReconnects=3;


    /**
     * 	Number of seconds to issue before falling back to master when failed over (when using multi-host failover).
     * 	Whichever condition is met first, 'queriesBeforeRetryMaster' or 'secondsBeforeRetryMaster' will cause an
     * 	attempt to be made to reconnect to the master. Defaults to 50
     */
    protected int secondsBeforeRetryMaster = 50;

    /**
     * 	Number of queries to issue before falling back to master when failed over (when using multi-host failover).
     * 	Whichever condition is met first, 'queriesBeforeRetryMaster' or 'secondsBeforeRetryMaster' will cause an
     * 	attempt to be made to reconnect to the master. Defaults to 30
     */
    protected int queriesBeforeRetryMaster = 30;

     /**
     * When using loadbalancing, the number of times the driver should cycle through available hosts, attempting to connect.
     * Between cycles, the driver will pause for 250ms if no servers are available.
     */
    protected int retriesAllDown = 120;
    protected long lastRetry = 0;

    protected int queriesSinceFailover=0;
    protected long secondaryHostFailTimestamp = 0;


    /* =========================== Failover variables ========================================= */
    protected long masterHostFailTimestamp = 0;
    protected int currentConnectionAttempts = 0;

    protected AtomicBoolean currentReadOnlyAsked=new AtomicBoolean();
    protected Protocol currentProtocol;

    private FailoverListener listener;

    public FailoverProxy(Protocol protocol, FailoverListener listener) {
        this.currentProtocol = protocol;
        this.listener = listener;
        this.listener.setProxy(this);
        parseHAOptions();
    }


    protected void parseHAOptions() {
        String s = currentProtocol.getInfo().getProperty("autoReconnect");
        if (s != null && s.equals("true")) autoReconnect = true;

        s = currentProtocol.getInfo().getProperty("autoReconnectForPools");
        if (s != null && s.equals("true")) autoReconnectForPools = true;

        s = currentProtocol.getInfo().getProperty("maxReconnects");
        if (s != null) maxReconnects = Integer.parseInt(s);

        s = currentProtocol.getInfo().getProperty("queriesBeforeRetryMaster");
        if (s != null) queriesBeforeRetryMaster = Integer.parseInt(s);

        s = currentProtocol.getInfo().getProperty("secondsBeforeRetryMaster");
        if (s != null) secondsBeforeRetryMaster = Integer.parseInt(s);

        s = currentProtocol.getInfo().getProperty("retriesAllDown");
        if (s != null) retriesAllDown = Integer.parseInt(s);
    }


    /**
     * proxy that catch Protocol call, to permit to catch errors and handle failover when multiple hosts
     * @param proxy the current protocol
     * @param method the called method on the protocol
     * @param args methods parameters
     * @return protocol method result
     * @throws Throwable the method throwed error if not catch by failover
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if ("initializeConnection".equals(methodName)) {
            this.listener.initializeConnection();
        }

        if ("executeQuery".equals(methodName)) {
            if (masterHostFailTimestamp!=0)queriesSinceFailover++;
            this.listener.preExecute();
        }
        if ("setReadonly".equals(methodName)) {
            this.listener.switchReadOnlyConnection((Boolean) args[0]);
        }

        try {
             Object returnObj = method.invoke(currentProtocol, args);
            if ("close".equals(methodName)) {
                this.listener.postClose();
            }
            return returnObj;
        } catch (InvocationTargetException e) {
            if (e.getTargetException() != null) {
                if (e.getTargetException() instanceof QueryException) {
                    if (hasToHandleFailover((QueryException) e.getTargetException())) {
                        HandleErrorResult handleErrorResult = handleFailover(method, args);
                        if (handleErrorResult.mustThrowError) throw e.getTargetException();
                        return handleErrorResult.resultObject;
                    }
                }
                throw e.getTargetException();
            }
            throw e;
        }
    }

    private HandleErrorResult handleFailover(Method method, Object[] args) throws Throwable {
        if (currentProtocol.isMasterConnection()) {
            //trying to connect of first error
            if (masterHostFailTimestamp == 0) {
                masterHostFailTimestamp = System.currentTimeMillis();
                currentConnectionAttempts = 0;
                return listener.primaryFail(method, args);
            }
            //if not first error, just launched error
            return new HandleErrorResult();
        } else {
            if (secondaryHostFailTimestamp == 0) {
                secondaryHostFailTimestamp = System.currentTimeMillis();
                currentConnectionAttempts = 0;
                return listener.secondaryFail(method, args);
            }
            return new HandleErrorResult();
        }
    }

    /**
     * Check if this Sqlerror is a connection exception. if that's the case, must be handle by failover
     *
     * error codes :
     * 08000 : connection exception
     * 08001 : SQL client unable to establish SQL connection
     * 08002 : connection name in use
     * 08003 : connection does not exist
     * 08004 : SQL server rejected SQL connection
     * 08006 : connection failure
     * 08007 : transaction resolution unknown
     *
     * @param e the Exception
     * @return true if there has been a connection error that must be handled by failover
     */
    public boolean hasToHandleFailover(QueryException e){
        if (e.getSqlState() != null && e.getSqlState().startsWith("08")) {
            return true;
        }
        return false;
    }

    protected void resetMasterFailoverData()  {
        currentConnectionAttempts = 0;
        masterHostFailTimestamp = 0;
        lastRetry = 0;
    }

    protected void resetSecondaryFailoverData() {
        currentConnectionAttempts = 0;
        secondaryHostFailTimestamp = 0;
        lastRetry = 0;
    }



}
