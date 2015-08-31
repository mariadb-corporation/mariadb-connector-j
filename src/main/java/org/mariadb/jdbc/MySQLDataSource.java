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


package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.SQLExceptionMapper;
import org.mariadb.jdbc.internal.common.DefaultOptions;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.UrlHAMode;
import org.mariadb.jdbc.internal.common.Utils;
import org.mariadb.jdbc.internal.mysql.*;
import org.slf4j.LoggerFactory;

import javax.sql.*;

import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;


public class MySQLDataSource implements DataSource, ConnectionPoolDataSource, XADataSource {
    private final static org.slf4j.Logger log = LoggerFactory.getLogger(MySQLDataSource.class);

    private final JDBCUrl jdbcUrl;

    public MySQLDataSource(String hostname, int port, String database) throws SQLException {
        ArrayList<HostAddress> hostAddresses = new ArrayList<HostAddress>();
        hostAddresses.add(new HostAddress(hostname, port));
        jdbcUrl = new JDBCUrl(database, hostAddresses, DefaultOptions.defaultValues(UrlHAMode.NONE), UrlHAMode.NONE);
    }

    public MySQLDataSource(String url) throws SQLException  {
        this.jdbcUrl = JDBCUrl.parse(url);
    }

    public MySQLDataSource() {
        ArrayList<HostAddress> hostAddresses = new ArrayList<>();
        hostAddresses.add(new HostAddress("localhost", 3306));
        JDBCUrl tmpJDBCUrl = null;
        try {
            tmpJDBCUrl = new JDBCUrl("", hostAddresses, DefaultOptions.defaultValues(UrlHAMode.NONE), UrlHAMode.NONE);
        } catch (SQLException e) {
            //cannot happen, but eating exception to avoid throw SQLException in class descriptor
        }
        jdbcUrl = tmpJDBCUrl;
    }

    /**
     * Sets the database name.
     *
     * @param dbName
     *            the name of the database
     */
    public void setDatabaseName(String dbName) {
        jdbcUrl.setDatabase(dbName);
    }

    /**
     * Gets the name of the database
     *
     * @return the name of the database for this data source
     */
    public String getDatabaseName() {
        return (jdbcUrl.getDatabase() != null) ? jdbcUrl.getDatabase() : "";
    }

    /**
     * Sets the username
     *
     * @param userName
     *            the username
     */
    public void setUser(String userName) {
        setUserName(userName);
    }

    /**
     * Gets the username
     *
     * @return the username to use when connecting to the database
     */
    public String getUser() {
        return jdbcUrl.getUsername();
    }

    /**
     * Sets the username
     *
     * @param userName
     *            the username
     */
    public void setUserName(String userName) {
        jdbcUrl.setUsername(userName);
    }

    /**
     * Gets the username
     *
     * @return the username to use when connecting to the database
     */
    public String getUserName() {
        return jdbcUrl.getUsername();
    }


    /**
     * Sets the password
     *
     * @param pass
     *            the password
     */
    public void setPassword(String pass) {
        jdbcUrl.setPassword(pass);
    }

    /**
     * Sets the database port.
     *
     * @param p
     *            the port
     */
    public void setPort(int p) {
        jdbcUrl.getHostAddresses().get(0).port = p;
    }

    /**
     * Returns the port number
     *
     * @return the port number
     */
    public int getPort() {
        return jdbcUrl.getHostAddresses().get(0).port;
    }

    /**
     * Sets the port number
     *
     * @param p
     *            the port
     *
     * @see #setPort
     */
    public void setPortNumber(int p) {
        if (p > 0) {
            setPort(p);
        }
    }

    /**
     * Returns the port number
     *
     * @return the port number
     */
    public int getPortNumber() {
        return getPort();
    }

    /**
     * Sets the server name.
     *
     * @param serverName
     *            the server name
     */
    public void setServerName(String serverName) {
        if (serverName != null && !serverName.isEmpty()) {
            jdbcUrl.getHostAddresses().get(0).host = serverName;
        }
    }

    public void setProperties(String properties) {
        jdbcUrl.setProperties(properties);
    }

    /**
     * Sets the connection string URL.
     *
     * @param url the connection string
     * @throws SQLException if error in URL
     */
    public void setURL(String url) throws SQLException {
        setUrl(url);
    }

    /**
     * Sets the connection string URL.
     * @param s the connection string
     * @throws SQLException if error in URL
     */
    public void setUrl(String s) throws SQLException {
        this.jdbcUrl.parseUrl(s);
    }

    /**
     * Returns the name of the database server
     *
     * @return the name of the database server
     */
    public String getServerName() {
        return (this.jdbcUrl.getHostAddresses().get(0).host != null) ? this.jdbcUrl.getHostAddresses().get(0).host : "";
    }

    /**
     * Attempts to establish a connection with the data source that this <code>DataSource</code> object represents.
     *
     * @return a connection to the data source
     * @throws java.sql.SQLException if a database access error occurs
     */
    public Connection getConnection() throws SQLException {
        try {
            ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
            Protocol proxyfiedProtocol = Utils.retrieveProxy(jdbcUrl, lock);
            return MySQLConnection.newConnection(proxyfiedProtocol, lock);
        } catch (QueryException e) {
            SQLExceptionMapper.throwException(e, null, null);
            return null;
        }
    }

    /**
     * Attempts to establish a connection with the data source that this <code>DataSource</code> object represents.
     *
     * @param username the database user on whose behalf the connection is being made
     * @param password the user's password
     * @return a connection to the data source
     * @throws java.sql.SQLException if a database access error occurs
     * @since 1.4
     */
    public Connection getConnection(final String username, final String password) throws SQLException {
        jdbcUrl.setUsername(username);
        jdbcUrl.setPassword(password);
        if (log.isDebugEnabled()) log.debug("connection : " +jdbcUrl.toString());
        return getConnection();
    }

    /**
     * Retrieves the log writer for this <code>DataSource</code> object.
     *
     * The log writer is a character output stream to which all logging and tracing messages for this data source
     * will be printed.  This includes messages printed by the methods of this object, messages printed by methods of
     * other objects manufactured by this object, and so on.  Messages printed to a data source specific log writer are
     * not printed to the log writer associated with the <code>java.sql.DriverManager</code> class.  When a
     * <code>DataSource</code> object is created, the log writer is initially null; in other words, the default is for
     * logging to be disabled.
     *
     * @return the log writer for this data source or null if logging is disabled
     * @throws java.sql.SQLException if a database access error occurs
     * @see #setLogWriter
     * @since 1.4
     */
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    /**
     * Sets the log writer for this <code>DataSource</code> object to the given <code>java.io.PrintWriter</code>
     * object.
     *
     * The log writer is a character output stream to which all logging and tracing messages for this data source
     * will be printed.  This includes messages printed by the methods of this object, messages printed by methods of
     * other objects manufactured by this object, and so on.  Messages printed to a data source- specific log writer are
     * not printed to the log writer associated with the <code>java.sql.DriverManager</code> class. When a
     * <code>DataSource</code> object is created the log writer is initially null; in other words, the default is for
     * logging to be disabled.
     *
     * @param out the new log writer; to disable logging, set to null
     * @throws java.sql.SQLException if a database access error occurs
     * @see #getLogWriter
     * @since 1.4
     */
    public void setLogWriter(final PrintWriter out) throws SQLException {

    }

    /**
     * Sets the maximum time in seconds that this data source will wait while attempting to connect to a database.  A
     * value of zero specifies that the timeout is the default system timeout if there is one; otherwise, it specifies
     * that there is no timeout. When a <code>DataSource</code> object is created, the login timeout is initially zero.
     *
     * @param seconds the data source login time limit
     * @throws java.sql.SQLException if a database access error occurs.
     * @see #getLoginTimeout
     * @since 1.4
     */
    public void setLoginTimeout(final int seconds) throws SQLException {

    }

    /**
     * Gets the maximum time in seconds that this data source can wait while attempting to connect to a database.  A
     * value of zero means that the timeout is the default system timeout if there is one; otherwise, it means that
     * there is no timeout. When a <code>DataSource</code> object is created, the login timeout is initially zero.
     *
     * @return the data source login time limit
     * @throws java.sql.SQLException if a database access error occurs.
     * @see #setLoginTimeout
     * @since 1.4
     */
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    /**
     * Returns an object that implements the given interface to allow access to non-standard methods, or standard
     * methods not exposed by the proxy.
     *
     * If the receiver implements the interface then the result is the receiver or a proxy for the receiver. If the
     * receiver is a wrapper and the wrapped object implements the interface then the result is the wrapped object or a
     * proxy for the wrapped object. Otherwise return the the result of calling <code>unwrap</code> recursively on the
     * wrapped object or a proxy for that result. If the receiver is not a wrapper and does not implement the interface,
     * then an <code>SQLException</code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException If no object found that implements the interface
     * @since 1.6
     */
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return null;
    }

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper for an
     * object that does. Returns false otherwise. If this implements the interface then return true, else if this is a
     * wrapper then return the result of recursively calling <code>isWrapperFor</code> on the wrapped object. If this
     * does not implement the interface and is not a wrapper, return false. This method should be implemented as a
     * low-cost operation compared to <code>unwrap</code> so that callers can use this method to avoid expensive
     * <code>unwrap</code> calls that may fail. If this method returns true then calling <code>unwrap</code> with the
     * same argument should succeed.
     *
     * @param iface a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws java.sql.SQLException if an error occurs while determining whether this is a wrapper for an object with
     *                               the given interface.
     * @since 1.6
     */
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return false;
    }

    /**
     * Attempts to establish a physical database connection that can
     * be used as a pooled connection.
     *
     * @return a <code>PooledConnection</code> object that is a physical
     *         connection to the database that this
     *         <code>ConnectionPoolDataSource</code> object represents
     * @throws java.sql.SQLException if a database access error occurs
     *                               if the JDBC driver does not support
     *                               this method
     * @since 1.4
     */
    public PooledConnection getPooledConnection() throws SQLException {
        return new MySQLPooledConnection((MySQLConnection)getConnection());
    }

    /**
     * Attempts to establish a physical database connection that can
     * be used as a pooled connection.
     *
     * @param user     the database user on whose behalf the connection is being made
     * @param password the user's password
     * @return a <code>PooledConnection</code> object that is a physical
     *         connection to the database that this
     *         <code>ConnectionPoolDataSource</code> object represents
     * @throws java.sql.SQLException if a database access error occurs
     * @since 1.4
     */
    public PooledConnection getPooledConnection(String user, String password) throws SQLException {
        return new MySQLPooledConnection((MySQLConnection)getConnection(user,password));
    }

    public XAConnection getXAConnection() throws SQLException {
        return new MySQLXAConnection((MySQLConnection)getConnection());
    }
    public XAConnection getXAConnection(String user, String password) throws SQLException {
        return new MySQLXAConnection((MySQLConnection)getConnection(user,password));
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // TODO Auto-generated method stub
        return null;
    }
}
