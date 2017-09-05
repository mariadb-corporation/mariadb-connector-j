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

package org.mariadb.jdbc;

import javax.sql.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;


public class MariaDbPooledConnection implements PooledConnection {

    protected final MariaDbConnection connection;
    private final List<ConnectionEventListener> connectionEventListeners;
    private final List<StatementEventListener> statementEventListeners;

    /**
     * Constructor.
     *
     * @param connection connection to retrieve connection options
     */
    public MariaDbPooledConnection(MariaDbConnection connection) {
        this.connection = connection;
        connection.pooledConnection = this;
        statementEventListeners = new CopyOnWriteArrayList<>();
        connectionEventListeners = new CopyOnWriteArrayList<>();
    }

    /**
     * Creates and returns a <code>Connection</code> object that is a handle
     * for the physical connection that
     * this <code>PooledConnection</code> object represents.
     * The connection pool manager calls this method when an application has
     * called the method <code>DataSource.getConnection</code> and there are
     * no <code>PooledConnection</code> objects available. See the
     * {@link PooledConnection interface description} for more information.
     *
     * @return a <code>Connection</code> object that is a handle to
     * this <code>PooledConnection</code> object
     * @throws SQLException if a database access error occurs
     *                      if the JDBC driver does not support
     *                      this method
     * @since 1.4
     */
    public Connection getConnection() throws SQLException {
        return connection;
    }

    /**
     * Closes the physical connection that this <code>PooledConnection</code>
     * object represents.  An application never calls this method directly;
     * it is called by the connection pool module, or manager.
     * <br>
     * See the {@link PooledConnection interface description} for more
     * information.
     *
     * @throws SQLException if a database access error occurs
     *                      if the JDBC driver does not support
     *                      this method
     * @since 1.4
     */
    public void close() throws SQLException {
        connection.pooledConnection = null;
        connection.close();
    }

    /**
     * Registers the given event failover so that it will be notified
     * when an event occurs on this <code>PooledConnection</code> object.
     *
     * @param listener a component, usually the connection pool manager,
     *                 that has implemented the
     *                 <code>ConnectionEventListener</code> interface and wants to be
     *                 notified when the connection is closed or has an error
     * @see #removeConnectionEventListener
     */
    public void addConnectionEventListener(ConnectionEventListener listener) {
        connectionEventListeners.add(listener);
    }

    /**
     * Removes the given event failover from the list of components that
     * will be notified when an event occurs on this
     * <code>PooledConnection</code> object.
     *
     * @param listener a component, usually the connection pool manager,
     *                 that has implemented the
     *                 <code>ConnectionEventListener</code> interface and
     *                 been registered with this <code>PooledConnection</code> object as
     *                 a failover
     * @see #addConnectionEventListener
     */
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        connectionEventListeners.remove(listener);
    }

    /**
     * Registers a <code>StatementEventListener</code> with this <code>PooledConnection</code> object.  Components that
     * wish to be notified when  <code>PreparedStatement</code>s created by the
     * connection are closed or are detected to be invalid may use this method
     * to register a <code>StatementEventListener</code> with this <code>PooledConnection</code> object.
     * <br>
     *
     * @param listener an component which implements the <code>StatementEventListener</code>
     *                 interface that is to be registered with this <code>PooledConnection</code> object
     *                 <br>
     * @since 1.6
     */
    public void addStatementEventListener(StatementEventListener listener) {
        statementEventListeners.add(listener);
    }

    /**
     * Removes the specified <code>StatementEventListener</code> from the list of
     * components that will be notified when the driver detects that a
     * <code>PreparedStatement</code> has been closed or is invalid.
     * <br>
     *
     * @param listener the component which implements the
     *                 <code>StatementEventListener</code> interface that was previously
     *                 registered with this <code>PooledConnection</code> object
     *                 <br>
     * @since 1.6
     */
    public void removeStatementEventListener(StatementEventListener listener) {
        statementEventListeners.remove(listener);
    }

    /**
     * Fire statement close event to listeners.
     *
     * @param st statement
     */
    public void fireStatementClosed(Statement st) {
        if (st instanceof PreparedStatement) {
            StatementEvent event = new StatementEvent(this, (PreparedStatement) st);
            for (StatementEventListener listener : statementEventListeners) {
                listener.statementClosed(event);
            }
        }
    }

    /**
     * Fire statement error to listeners.
     *
     * @param st statement
     * @param ex exception
     */
    public void fireStatementErrorOccured(Statement st, SQLException ex) {
        if (st instanceof PreparedStatement) {
            StatementEvent event = new StatementEvent(this, (PreparedStatement) st, ex);
            for (StatementEventListener listener : statementEventListeners) {
                listener.statementErrorOccurred(event);
            }
        }
    }

    /**
     * Fire Connection close to listening listeners.
     */
    public void fireConnectionClosed() {
        ConnectionEvent event = new ConnectionEvent(this);
        for (ConnectionEventListener listener : connectionEventListeners) {
            listener.connectionClosed(event);
        }
    }

    /**
     * Fire connection error to listening listerners.
     *
     * @param ex exception
     */
    public void fireConnectionErrorOccured(SQLException ex) {
        ConnectionEvent event = new ConnectionEvent(this, ex);
        for (ConnectionEventListener listener : connectionEventListeners) {
            listener.connectionErrorOccurred(event);
        }
    }

    /**
     * Indicate if there are any registered listener.
     * @return true if no listener.
     */
    public boolean noStmtEventListeners() {
        return statementEventListeners.isEmpty();
    }
}
