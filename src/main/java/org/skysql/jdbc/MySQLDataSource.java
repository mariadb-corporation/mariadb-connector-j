/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *  Neither the name of the driver nor the names of its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.skysql.jdbc;

import org.skysql.jdbc.internal.SQLExceptionMapper;
import org.skysql.jdbc.internal.common.QueryException;
import org.skysql.jdbc.internal.common.query.MySQLQueryFactory;
import org.skysql.jdbc.internal.mysql.MySQLProtocol;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * User: marcuse Date: Feb 7, 2009 Time: 10:53:22 PM
 */
public class MySQLDataSource implements DataSource {
    private final String hostname;
    private final int port;
    private final String database;

    public MySQLDataSource(final String hostname, final int port, final String database) {
        this.hostname = hostname;
        this.port = port;
        this.database = database;
    }

    /**
     * <p>Attempts to establish a connection with the data source that this <code>DataSource</code> object represents.
     *
     * @return a connection to the data source
     * @throws java.sql.SQLException if a database access error occurs
     */
    public Connection getConnection() throws SQLException {
        try {
            return new MySQLConnection(new MySQLProtocol(hostname, port, database, null, null, new Properties()),
                    new MySQLQueryFactory());
        } catch (QueryException e) {
            throw SQLExceptionMapper.get(e);
        }
    }

    /**
     * <p>Attempts to establish a connection with the data source that this <code>DataSource</code> object represents.
     *
     * @param username the database user on whose behalf the connection is being made
     * @param password the user's password
     * @return a connection to the data source
     * @throws java.sql.SQLException if a database access error occurs
     * @since 1.4
     */
    public Connection getConnection(final String username, final String password) throws SQLException {
        try {
            return new MySQLConnection(new MySQLProtocol(hostname, port, database, username, password, new Properties()),
                    new MySQLQueryFactory());
        } catch (QueryException e) {
            throw SQLExceptionMapper.get(e);
        }
    }

    /**
     * <p>Retrieves the log writer for this <code>DataSource</code> object.
     * <p/>
     * <p>The log writer is a character output stream to which all logging and tracing messages for this data source
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
     * <p>Sets the log writer for this <code>DataSource</code> object to the given <code>java.io.PrintWriter</code>
     * object.
     * <p/>
     * <p>The log writer is a character output stream to which all logging and tracing messages for this data source
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
     * <p>Sets the maximum time in seconds that this data source will wait while attempting to connect to a database.  A
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
     * <p/>
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
}
