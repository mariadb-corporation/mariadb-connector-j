// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc;

import java.io.Closeable;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.List;
import java.util.logging.Logger;
import javax.sql.*;
import org.mariadb.jdbc.pool.Pool;
import org.mariadb.jdbc.pool.Pools;

/** MariaDB pool datasource. This use mariadb internal pool. */
public class MariaDbPoolDataSource
    implements DataSource, ConnectionPoolDataSource, XADataSource, Closeable, AutoCloseable {

  private Pool pool;
  private Configuration conf = null;
  private String url = null;
  private String user = null;
  private String password = null;
  private Integer loginTimeout = null;

  /** Constructor */
  public MariaDbPoolDataSource() {}

  /**
   * Constructor with url
   *
   * @param url connection string
   * @throws SQLException if configuration fails
   */
  public MariaDbPoolDataSource(String url) throws SQLException {
    if (Configuration.acceptsUrl(url)) {
      this.url = url;
      conf = Configuration.parse(url);
      pool = Pools.retrievePool(conf);
    } else {
      throw new SQLException(String.format("Wrong mariaDB url: %s", url));
    }
  }

  private void config() throws SQLException {
    if (url == null) throw new SQLException("url not set");
    conf = Configuration.parse(url);
    if (loginTimeout != null) conf.connectTimeout(loginTimeout * 1000);
    if (user != null || password != null) {
      conf = conf.clone(user, password);
    }
    if (user != null) {
      user = conf.user();
    }
    if (password != null) {
      password = conf.password();
    }

    pool = Pools.retrievePool(conf);
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
    if (conf == null) config();
    return pool.getPoolConnection().getConnection();
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
    if (conf == null) config();
    return pool.getPoolConnection(username, password).getConnection();
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
    if (loginTimeout != null) return loginTimeout;
    if (conf != null) return conf.connectTimeout() / 1000;
    return DriverManager.getLoginTimeout() > 0 ? DriverManager.getLoginTimeout() : 30;
  }

  /**
   * Sets the maximum time in seconds that this data source will wait while attempting to connect to
   * a database. A value of zero specifies that the timeout is the default system timeout if there
   * is one; otherwise, it specifies that there is no timeout. When a <code>DataSource</code> object
   * is created, the login timeout is initially 30s.
   *
   * @param seconds the data source login time limit
   * @throws SQLException if wrong configuration set
   * @see #getLoginTimeout
   */
  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    loginTimeout = seconds;
    if (conf != null) config();
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
    if (conf == null) config();
    return pool.getPoolConnection();
  }

  @Override
  public PooledConnection getPooledConnection(String username, String password)
      throws SQLException {
    if (conf == null) config();
    return pool.getPoolConnection(username, password);
  }

  @Override
  public XAConnection getXAConnection() throws SQLException {
    if (conf == null) config();
    return pool.getPoolConnection();
  }

  @Override
  public XAConnection getXAConnection(String username, String password) throws SQLException {
    if (conf == null) config();
    return pool.getPoolConnection(username, password);
  }

  /**
   * Sets the URL for this datasource
   *
   * @param url connection string
   * @throws SQLException if url is not accepted
   */
  public void setUrl(String url) throws SQLException {
    if (Configuration.acceptsUrl(url)) {
      this.url = url;
      config();
    } else {
      throw new SQLException(String.format("Wrong mariaDB url: %s", url));
    }
  }

  /**
   * Returns the URL for this datasource
   *
   * @return the URL for this datasource
   */
  public String getUrl() {
    if (conf == null) return url;
    return conf.initialUrl();
  }

  /**
   * return user
   *
   * @return user
   */
  public String getUser() {
    return user;
  }

  /**
   * Set user
   *
   * @param user user
   * @throws SQLException if configuration fails
   */
  public void setUser(String user) throws SQLException {
    this.user = user;
    if (conf != null) config();
  }

  /**
   * set password
   *
   * @param password password
   * @throws SQLException if configuration fails
   */
  public void setPassword(String password) throws SQLException {
    this.password = password;
    if (conf != null) config();
  }

  /** Close datasource. */
  public void close() {
    pool.close();
  }

  /**
   * get pool name
   *
   * @return pool name
   */
  public String getPoolName() {
    return (pool != null) ? pool.getPoolTag() : null;
  }

  /**
   * Get current idle threads. !! For testing purpose only !!
   *
   * @return current thread id's
   */
  public List<Long> testGetConnectionIdleThreadIds() {
    return (pool != null) ? pool.testGetConnectionIdleThreadIds() : null;
  }
}
