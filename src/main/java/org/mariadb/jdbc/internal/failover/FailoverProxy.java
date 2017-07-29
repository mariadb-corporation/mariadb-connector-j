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

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;
import org.mariadb.jdbc.internal.util.exceptions.ExceptionMapper;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;


public class FailoverProxy implements InvocationHandler {
    public static final String METHOD_IS_EXPLICIT_CLOSED = "isExplicitClosed";
    public static final String METHOD_GET_OPTIONS = "getOptions";
    public static final String METHOD_GET_PROXY = "getProxy";
    public static final String METHOD_EXECUTE_QUERY = "executeQuery";
    public static final String METHOD_SET_READ_ONLY = "setReadonly";
    public static final String METHOD_IS_READ_ONLY = "isReadOnly";
    public static final String METHOD_CLOSED_EXPLICIT = "closeExplicit";
    public static final String METHOD_IS_CLOSED = "isClosed";
    public static final String METHOD_EXECUTE_PREPARED_QUERY = "executePreparedQuery";
    public static final String METHOD_COM_MULTI_PREPARE_EXECUTES = "prepareAndExecutesComMulti";
    public static final String METHOD_PROLOG_PROXY = "prologProxy";
    private static Logger logger = LoggerFactory.getLogger(FailoverProxy.class);
    public final ReentrantLock lock;

    private Listener listener;

    /**
     * Proxy constructor.
     *
     * @param listener failover implementation.
     * @param lock     synchronisation lock
     * @throws SQLException if connection error occur
     */
    public FailoverProxy(Listener listener, ReentrantLock lock) throws SQLException {
        this.lock = lock;
        this.listener = listener;
        this.listener.setProxy(this);
        this.listener.initializeConnection();
    }

    /**
     * Add Host information ("on HostAddress...") to exception.
     * <p>
     * example :
     * <p>
     * java.sql.SQLException: (conn:603) Cannot execute statement in a READ ONLY transaction.<br/>
     * Query is: INSERT INTO TableX VALUES (21)<br/>
     * on HostAddress{host='mydb.example.com', port=3306},master=true</p>
     *
     * @param exception current exception
     * @param protocol  protocol to have hostname
     */
    private static SQLException addHostInformationToException(SQLException exception, Protocol protocol) {
        if (protocol != null) {
            return new SQLException(exception.getMessage()
                    + "\non " + protocol.getHostAddress().toString() + ",master="
                    + protocol.isMasterConnection(), exception.getSQLState(), exception.getErrorCode(), exception.getCause());

        }
        return exception;
    }

    /**
     * Proxy that catch Protocol call, to permit to catch errors and handle failover when multiple hosts.
     *
     * @param proxy  the current protocol
     * @param method the called method on the protocol
     * @param args   methods parameters
     * @return protocol method result
     * @throws Throwable the method throwed error if not catch by failover
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();
        switch (methodName) {
            case METHOD_IS_EXPLICIT_CLOSED:
                return listener.isExplicitClosed();
            case METHOD_GET_OPTIONS:
                return listener.getUrlParser().getOptions();
            case METHOD_GET_PROXY:
                return this;
            case METHOD_IS_CLOSED:
                return listener.isClosed();
            case METHOD_EXECUTE_QUERY:
                try {
                    this.listener.preExecute();
                } catch (SQLException e) {
                    //handle failover only if connection error
                    //normal error can be thrown upon reconnection if there was a transaction in progress.
                    if (hasToHandleFailover(e)) {
                        return handleFailOver(e, method, args, listener.getCurrentProtocol());
                    }
                }
                break;
            case METHOD_SET_READ_ONLY:
                this.listener.switchReadOnlyConnection((Boolean) args[0]);
                return null;
            case METHOD_IS_READ_ONLY:
                return this.listener.isReadOnly();
            case METHOD_CLOSED_EXPLICIT:
                this.listener.preClose();
                return null;
            case METHOD_COM_MULTI_PREPARE_EXECUTES:
            case METHOD_EXECUTE_PREPARED_QUERY:
                boolean mustBeOnMaster = (Boolean) args[0];
                ServerPrepareResult serverPrepareResult = (ServerPrepareResult) args[1];
                if (serverPrepareResult != null) {
                    if (!mustBeOnMaster && serverPrepareResult.getUnProxiedProtocol().isMasterConnection() && !this.listener.hasHostFail()) {
                        //PrepareStatement was to be executed on slave, but since a failover was running on master connection. Slave connection is up
                        // again, so has to be re-prepared on slave
                        try {
                            logger.trace("re-prepare query \"" + serverPrepareResult.getSql() + "\" on slave (was "
                                    + "temporary on master since failover)");
                            this.listener.rePrepareOnSlave(serverPrepareResult, mustBeOnMaster);
                        } catch (SQLException q) {
                            //error during re-prepare, will do executed on master.
                        }
                    }
                    try {
                        return listener.invoke(method, args, serverPrepareResult.getUnProxiedProtocol());
                    } catch (InvocationTargetException e) {
                        if (e.getTargetException() != null) {
                            if (e.getTargetException() instanceof SQLException) {
                                if (hasToHandleFailover((SQLException) e.getTargetException())) {
                                    return handleFailOver((SQLException) e.getTargetException(), method, args,
                                            serverPrepareResult.getUnProxiedProtocol());
                                }
                            }
                            throw e.getTargetException();
                        }
                        throw e;
                    }
                }
                break;
            case METHOD_PROLOG_PROXY:
                try {
                    if (args[0] != null) {
                        return listener.invoke(method, args, ((ServerPrepareResult) args[0]).getUnProxiedProtocol());
                    }
                } catch (InvocationTargetException e) {
                    if (e.getTargetException() != null) {
                        if (e.getTargetException() instanceof SQLException) {
                            if (hasToHandleFailover((SQLException) e.getTargetException())) {
                                return handleFailOver((SQLException) e.getTargetException(), method, args,
                                        ((ServerPrepareResult) args[0]).getUnProxiedProtocol());
                            }
                        }
                        throw e.getTargetException();
                    }
                    throw e;
                }

            default:
        }

        return executeInvocation(method, args, false);

    }

    private Object executeInvocation(Method method, Object[] args, boolean isSecondExecution) throws Throwable {

        try {
            return listener.invoke(method, args);
        } catch (InvocationTargetException e) {
            if (e.getTargetException() != null) {
                if (e.getTargetException() instanceof SQLException) {

                    SQLException queryException = (SQLException) e.getTargetException();
                    Protocol protocol = listener.getCurrentProtocol();

                    queryException = addHostInformationToException(queryException, protocol);

                    //check that failover is due to kill command
                    boolean killCmd = queryException != null
                            && queryException.getSQLState() != null
                            && queryException.getSQLState().equals("70100")
                            && 1927 == queryException.getErrorCode();

                    if (killCmd) {
                        handleFailOver(queryException, method, args, protocol);
                        return null;
                    }

                    if (hasToHandleFailover(queryException)) {
                        return handleFailOver(queryException, method, args, protocol);
                    }

                    //error is "The MySQL server is running with the %s option so it cannot execute this statement"
                    //checking that server was master has not been demote to slave without resetting connections
                    if (queryException.getErrorCode() == 1290
                            && !isSecondExecution
                            && protocol != null
                            && protocol.isMasterConnection()
                            && !protocol.checkIfMaster()) {

                        boolean inTransaction = protocol.inTransaction();
                        boolean isReconnected;

                        //connection state has changed, master connection is now read-only
                        //reconnect to master, to re-execute command if wasn't in a transaction since
                        //we are sure has not been executed.

                        //reconnection
                        lock.lock();
                        try {
                            protocol.close();
                            isReconnected = listener.primaryFail(null, null, false).isReconnected;
                        } finally {
                            lock.unlock();
                        }

                        //relaunch command
                        if (isReconnected && !inTransaction) {
                            return executeInvocation(method, args, true);
                        }

                        //throw exception if not reconnected, or was in a transaction
                        return handleFailOver(queryException, method, args, listener.getCurrentProtocol());

                    }
                }
                throw e.getTargetException();
            }
            throw e;
        }
    }

    /**
     * After a connection exception, launch failover.
     *
     * @param qe     the exception thrown
     * @param method the method to call if failover works well
     * @param args   the arguments of the method
     * @return the object return from the method
     * @throws Throwable throwable
     */
    private Object handleFailOver(SQLException qe, Method method, Object[] args, Protocol protocol) throws Throwable {
        HostAddress failHostAddress = null;
        boolean failIsMaster = true;
        if (protocol != null) {
            failHostAddress = protocol.getHostAddress();
            failIsMaster = protocol.isMasterConnection();
        }

        HandleErrorResult handleErrorResult = listener.handleFailover(qe, method, args, protocol);
        if (handleErrorResult.mustThrowError) {
            listener.throwFailoverMessage(failHostAddress, failIsMaster, qe, handleErrorResult.isReconnected);
        }
        return handleErrorResult.resultObject;
    }

    /**
     * Check if this Sqlerror is a connection exception. if that's the case, must be handle by failover
     * <p>
     * error codes :
     * 08000 : connection exception
     * 08001 : SQL client unable to establish SQL connection
     * 08002 : connection name in use
     * 08003 : connection does not exist
     * 08004 : SQL server rejected SQL connection
     * 08006 : connection failure
     * 08007 : transaction resolution unknown
     * 70100 : connection was killed if error code is "1927"
     *
     * @param exception the Exception
     * @return true if there has been a connection error that must be handled by failover
     */
    public boolean hasToHandleFailover(SQLException exception) {
        return exception.getSQLState() != null
                && (exception.getSQLState().startsWith("08")
                || (exception.getSQLState().equals("70100") && 1927 == exception.getErrorCode())) ;
    }

    /**
     * Launch reconnect implementation.
     *
     * @throws SQLException exception
     */
    public void reconnect() throws SQLException {
        try {
            listener.reconnect();
        } catch (SQLException e) {
            ExceptionMapper.throwException(e, null, null);
        }
    }

    public Listener getListener() {
        return listener;
    }
}
