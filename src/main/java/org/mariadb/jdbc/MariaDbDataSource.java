// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc;

import java.io.PrintWriter;
import java.sql.*;
import java.sql.Connection;
import java.util.logging.Logger;
import javax.sql.*;

public class MariaDbDataSource implements DataSource, ConnectionPoolDataSource, XADataSource {

  private final Configuration conf;

  public MariaDbDataSource(String url) throws SQLException {
    if (Configuration.acceptsUrl(url)) {
      conf = Configuration.parse(url);
    } else {
      throw new SQLException(String.format("Wrong mariaDB url: %s", url));
    }
  }

  /**
   * Attempts to establish a connection with the data source that this {@code DataSource} object
   * represents.
   *
   * @return a connection to the data source
   * @throws SQLException if a database access error occurs
   * @throws SQLTimeoutException when the driver has determined that the timeout value specified by
   *     the {@code setLoginTimeout} method has been exceeded and has at least tried to cancel the
   *     current database connection attempt
   */
  @Override
  public Connection getConnection() throws SQLException {
    return Driver.connect(conf);
  }

  /**
   * Attempts to establish a connection with the data source that this {@code DataSource} object
   * represents.
   *
   * @param username the database user on whose behalf the connection is being made
   * @param password the user's password
   * @return a connection to the data source
   * @throws SQLException if a database access error occurs
   * @throws SQLTimeoutException when the driver has determined that the timeout value specified by
   *     the {@code setLoginTimeout} method has been exceeded and has at least tried to cancel the
   *     current database connection attempt
   */
  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    Configuration conf = this.conf.clone(username, password);
    return Driver.connect(conf);
  }

  /**
   * Returns an object that implements the given interface to allow access to non-standard methods,
   * or standard methods not exposed by the proxy.
   *
   * <p>If the receiver implements the interface then the result is the receiver or a proxy for the
   * receiver. If the receiver is a wrapper and the wrapped object implements the interface then the
   * result is the wrapped object or a proxy for the wrapped object. Otherwise, return the result of
   * calling <code>unwrap</code> recursively on the wrapped object or a proxy for that result. If
   * the receiver is not a wrapper and does not implement the interface, then an <code>SQLException
   * </code> is thrown.
   *
   * @param iface A Class defining an interface that the result must implement.
   * @return an object that implements the interface. Maybe a proxy for the actual implementing
   *     object.
   * @throws SQLException If no object found that implements the interface
   */
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (isWrapperFor(iface)) {
      return iface.cast(this);
    }
    throw new SQLException("Datasource is not a wrapper for " + iface.getName());
  }

  /**
   * Returns true if this either implements the interface argument or is directly or indirectly a
   * wrapper for an object that does. Returns false otherwise. If this implements the interface then
   * return true, else if this is a wrapper then return the result of recursively calling <code>
   * isWrapperFor</code> on the wrapped object. If this does not implement the interface and is not
   * a wrapper, return false. This method should be implemented as a low-cost operation compared to
   * <code>unwrap</code> so that callers can use this method to avoid expensive <code>unwrap</code>
   * calls that may fail. If this method returns true then calling <code>unwrap</code> with the same
   * argument should succeed.
   *
   * @param iface a Class defining an interface.
   * @return true if this implements the interface or directly or indirectly wraps an object that
   *     does.
   */
  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isInstance(this);
  }

  /**
   * Implementation doesn't use logwriter
   *
   * @return the log writer for this data source or null if logging is disabled
   * @see #setLogWriter
   */
  @Override
  public PrintWriter getLogWriter() {
    return null;
  }

  /**
   * Implementation doesn't use logwriter
   *
   * @param out the new log writer; to disable logging, set to null
   * @see #getLogWriter
   */
  @Override
  public void setLogWriter(PrintWriter out) {}

  /**
   * Gets the maximum time in seconds that this data source can wait while attempting to connect to
   * a database. A value of zero means that the timeout is the default system timeout if there is
   * one; otherwise, it means that there is no timeout. When a <code>DataSource</code> object is
   * created, the login timeout is initially to 30s.
   *
   * @return the data source login time limit
   * @see #setLoginTimeout
   */
  @Override
  public int getLoginTimeout() {
    return conf.connectTimeout() / 1000;
  }

  /**
   * Sets the maximum time in seconds that this data source will wait while attempting to connect to
   * a database. A value of zero specifies that the timeout is the default system timeout if there
   * is one; otherwise, it specifies that there is no timeout. When a <code>DataSource</code> object
   * is created, the login timeout is initially 30s.
   *
   * @param seconds the data source login time limit
   * @see #getLoginTimeout
   * @since 1.4
   */
  @Override
  public void setLoginTimeout(int seconds) {
    conf.connectTimeout(seconds * 1000);
  }

  /**
   * Not implemented
   *
   * @return the parent Logger for this data source
   */
  @Override
  public Logger getParentLogger() {
    return null;
  }

  @Override
  public PooledConnection getPooledConnection() throws SQLException {
    return new MariaDbPoolConnection(Driver.connect(conf));
  }

  @Override
  public PooledConnection getPooledConnection(String username, String password)
      throws SQLException {
    Configuration conf = this.conf.clone(username, password);
    return new MariaDbPoolConnection(Driver.connect(conf));
  }

  @Override
  public XAConnection getXAConnection() throws SQLException {
    return new MariaDbPoolConnection(Driver.connect(conf));
  }

  @Override
  public XAConnection getXAConnection(String username, String password) throws SQLException {
    Configuration conf = this.conf.clone(username, password);
    return new MariaDbPoolConnection(Driver.connect(conf));
  }
}
