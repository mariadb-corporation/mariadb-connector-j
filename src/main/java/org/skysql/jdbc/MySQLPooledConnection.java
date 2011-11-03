package org.skysql.jdbc;

import javax.sql.*;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class MySQLPooledConnection implements  PooledConnection{

    MySQLConnection connection;
    List<ConnectionEventListener> connectionEventListeners;
    List<StatementEventListener> statementEventListeners;

    public MySQLPooledConnection(MySQLConnection connection)
    {
       this.connection = connection;
       connection.pooledConnection = this;
       statementEventListeners = new ArrayList<StatementEventListener>();
       connectionEventListeners = new ArrayList<ConnectionEventListener>();
    }
    /**
     * Creates and returns a <code>Connection</code> object that is a handle
     * for the physical connection that
     * this <code>PooledConnection</code> object represents.
     * The connection pool manager calls this method when an application has
     * called the method <code>DataSource.getConnection</code> and there are
     * no <code>PooledConnection</code> objects available. See the
     * {@link javax.sql.PooledConnection interface description} for more information.
     *
     * @return a <code>Connection</code> object that is a handle to
     *         this <code>PooledConnection</code> object
     * @throws java.sql.SQLException if a database access error occurs
     * @throws SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support
     *                               this method
     * @since 1.4
     */
    public Connection getConnection() throws SQLException {
        return connection;
    }

    /**
     * Closes the physical connection that this <code>PooledConnection</code>
     * object represents.  An application never calls this method directly;
     * it is called by the connection pool module, or manager.
     * <p/>
     * See the {@link javax.sql.PooledConnection interface description} for more
     * information.
     *
     * @throws java.sql.SQLException if a database access error occurs
     * @throws SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support
     *                               this method
     * @since 1.4
     */
    public void close() throws SQLException {
        connection.close();
    }

    /**
     * Registers the given event listener so that it will be notified
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
     * Removes the given event listener from the list of components that
     * will be notified when an event occurs on this
     * <code>PooledConnection</code> object.
     *
     * @param listener a component, usually the connection pool manager,
     *                 that has implemented the
     *                 <code>ConnectionEventListener</code> interface and
     *                 been registered with this <code>PooledConnection</code> object as
     *                 a listener
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
     * <p/>
     *
     * @param listener an component which implements the <code>StatementEventListener</code>
     *                 interface that is to be registered with this <code>PooledConnection</code> object
     *                 <p/>
     * @since 1.6
     */
    public void addStatementEventListener(StatementEventListener listener) {
        statementEventListeners.add(listener);
    }

    /**
     * Removes the specified <code>StatementEventListener</code> from the list of
     * components that will be notified when the driver detects that a
     * <code>PreparedStatement</code> has been closed or is invalid.
     * <p/>
     *
     * @param listener the component which implements the
     *                 <code>StatementEventListener</code> interface that was previously
     *                 registered with this <code>PooledConnection</code> object
     *                 <p/>
     * @since 1.6
     */
    public void removeStatementEventListener(StatementEventListener listener) {
       statementEventListeners.remove(listener);
    }

    public void fireStatementClosed(Statement st) {
        if (st instanceof PreparedStatement) {
            StatementEvent event = new StatementEvent(this, (PreparedStatement)st);
            for(StatementEventListener listener:statementEventListeners)
                listener.statementClosed(event);
        }
    }

    public void fireStatementErrorOccured(Statement st, SQLException e) {
        if (st instanceof PreparedStatement) {
            StatementEvent event = new StatementEvent(this,(PreparedStatement) st,e);
            for(StatementEventListener listener:statementEventListeners)
                listener.statementErrorOccurred(event);
        }
    }

    public void fireConnectionClosed() {
        ConnectionEvent event = new ConnectionEvent(this);
        for(ConnectionEventListener listener: connectionEventListeners)
           listener.connectionClosed(event);
    }

    public void fireConnectionErrorOccured(SQLException e) {
        ConnectionEvent event = new ConnectionEvent(this,e);
        for(ConnectionEventListener listener: connectionEventListeners)
           listener.connectionErrorOccurred(event);
    }
}
