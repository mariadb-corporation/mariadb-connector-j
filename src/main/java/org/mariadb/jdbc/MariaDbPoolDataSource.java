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
 */


package org.mariadb.jdbc;

import java.io.Closeable;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import org.mariadb.jdbc.internal.util.DefaultOptions;
import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.util.constant.HaMode;
import org.mariadb.jdbc.internal.util.exceptions.ExceptionMapper;
import org.mariadb.jdbc.internal.util.pool.Pool;
import org.mariadb.jdbc.internal.util.pool.Pools;


public class MariaDbPoolDataSource implements DataSource, XADataSource, Closeable, AutoCloseable {

  private UrlParser urlParser;
  private Pool pool;

  private String hostname;
  private Integer port;
  private Integer connectTimeout;
  private String database;
  private String url;
  private String user;
  private String password;
  private String poolName;
  private Integer maxPoolSize;
  private Integer minPoolSize;
  private Integer maxIdleTime;
  private Boolean staticGlobal;
  private Integer poolValidMinDelay;

  /**
   * Constructor.
   *
   * @param hostname hostname (ipv4, ipv6, dns name)
   * @param port     server port
   * @param database database name
   */
  public MariaDbPoolDataSource(String hostname, int port, String database) {
    this.hostname = hostname;
    this.port = port;
    this.database = database;
  }

  public MariaDbPoolDataSource(String url) {
    this.url = url;
  }

  /**
   * Default constructor. hostname will be localhost, port 3306.
   */
  public MariaDbPoolDataSource() {

  }

  /**
   * Gets the name of the database.
   *
   * @return the name of the database for this data source
   */
  public String getDatabaseName() {
    if (database != null) {
      return database;
    }
    return (urlParser != null && urlParser.getDatabase() != null) ? urlParser.getDatabase() : "";
  }

  /**
   * Sets the database name.
   *
   * @param database the name of the database
   * @throws SQLException if error in URL
   */
  public void setDatabaseName(String database) throws SQLException {
    checkNotInitialized();
    this.database = database;
  }

  private void checkNotInitialized() throws SQLException {
    if (pool != null) {
      throw new SQLException("can not perform a configuration change once initialized");
    }
  }

  /**
   * Gets the username.
   *
   * @return the username to use when connecting to the database
   */
  public String getUser() {
    if (user != null) {
      return user;
    }
    return urlParser != null ? urlParser.getUsername() : null;
  }

  /**
   * Sets the username.
   *
   * @param user the username
   * @throws SQLException if error in URL
   */
  public void setUser(String user) throws SQLException {
    checkNotInitialized();
    this.user = user;
  }

  /**
   * Sets the password.
   *
   * @param password the password
   * @throws SQLException if error in URL
   */
  public void setPassword(String password) throws SQLException {
    checkNotInitialized();
    this.password = password;
  }

  /**
   * Returns the port number.
   *
   * @return the port number
   */
  public int getPort() {
    if (port != null && port != 0) {
      return port;
    }
    return urlParser != null ? urlParser.getHostAddresses().get(0).port : 3306;
  }

  /**
   * Sets the database port.
   *
   * @param port the port
   * @throws SQLException if error in URL
   */
  public void setPort(int port) throws SQLException {
    checkNotInitialized();
    this.port = port;
  }

  /**
   * Returns the port number.
   *
   * @return the port number
   */
  public int getPortNumber() {
    return getPort();
  }

  /**
   * Sets the port number.
   *
   * @param port the port
   * @throws SQLException if error in URL
   * @see #setPort
   */
  public void setPortNumber(int port) throws SQLException {
    checkNotInitialized();
    if (port > 0) {
      setPort(port);
    }
  }

  /**
   * Sets the connection string URL.
   *
   * @param url the connection string
   * @throws SQLException if error in URL
   */
  public void setUrl(String url) throws SQLException {
    checkNotInitialized();
    this.url = url;
  }

  /**
   * Returns the name of the database server.
   *
   * @return the name of the database server
   */
  public String getServerName() {
    if (hostname != null) {
      return hostname;
    }
    boolean hasHost = urlParser != null && this.urlParser.getHostAddresses().get(0).host != null;
    return (hasHost) ? this.urlParser.getHostAddresses().get(0).host : "localhost";
  }

  /**
   * Sets the server name.
   *
   * @param serverName the server name
   * @throws SQLException if error in URL
   **/
  public void setServerName(String serverName) throws SQLException {
    checkNotInitialized();
    hostname = serverName;
  }

  /**
   * Attempts to establish a connection with the data source that this <code>DataSource</code>
   * object represents.
   *
   * @return a connection to the data source
   * @throws SQLException if a database access error occurs
   */
  public Connection getConnection() throws SQLException {
    try {
      if (pool == null) {
        initialize();
      }
      return pool.getConnection();
    } catch (SQLException e) {
      throw ExceptionMapper.getException(e, null, null, false);
    }
  }

  /**
   * Attempts to establish a connection with the data source that this <code>DataSource</code>
   * object represents.
   *
   * @param username the database user on whose behalf the connection is being made
   * @param password the user's password
   * @return a connection to the data source
   * @throws SQLException if a database access error occurs
   */
  public Connection getConnection(final String username, final String password)
      throws SQLException {
    try {
      if (pool == null) {
        this.user = username;
        this.password = password;

        initialize();
        return pool.getConnection();
      }

      if ((urlParser.getUsername() != null ? urlParser.getUsername().equals(username)
          : username == null)
          && (urlParser.getPassword() != null ? urlParser.getPassword().equals(password)
          : (password == null || password.isEmpty()))) {
        return pool.getConnection();
      }

      //username / password are different from the one already used to initialize pool
      //-> return a real new connection.

      UrlParser urlParser = (UrlParser) this.urlParser.clone();
      urlParser.setUsername(username);
      urlParser.setPassword(password);
      return MariaDbConnection.newConnection(urlParser, pool.getGlobalInfo());

    } catch (SQLException e) {
      throw ExceptionMapper.getException(e, null, null, false);
    } catch (CloneNotSupportedException cloneException) {
      throw new SQLException("Error in configuration");
    }
  }

  /**
   * Retrieves the log writer for this <code>DataSource</code> object.
   *
   * <p>The log writer is a character output stream to which all logging and tracing messages for
   * this data source will be printed.  This includes messages printed by the methods of this
   * object, messages printed by methods of other objects manufactured by this object, and so on.
   * Messages printed to a data source specific log writer are not printed to the log writer
   * associated with the <code>java.sql.DriverManager</code> class.</p>
   *
   * <p>When a <code>DataSource</code> object is created, the log writer is initially null; in other words,
   * the default is for logging to be disabled.</p>
   *
   * @return the log writer for this data source or null if logging is disabled
   * @see #setLogWriter
   */
  public PrintWriter getLogWriter() {
    return null;
  }

  /**
   * Sets the log writer for this <code>DataSource</code> object to the given
   * <code>java.io.PrintWriter</code> object.
   *
   * <p>The log writer is a character output stream to which all logging and tracing messages for this
   * data source will be printed.  This includes messages printed by the methods of this object,
   * messages printed by methods of other objects manufactured by this object, and so on.  Messages
   * printed to a data source- specific log writer are not printed to the log writer associated with
   * the <code>java.sql.DriverManager</code> class. When a
   * <code>DataSource</code> object is created the log writer is initially null; in other words,
   * the default is for logging to be disabled.</p>
   *
   * @param out the new log writer; to disable logging, set to null
   * @see #getLogWriter
   * @since 1.4
   */
  public void setLogWriter(final PrintWriter out) {
    //not implemented
  }

  /**
   * Gets the maximum time in seconds that this data source can wait while attempting to connect to
   * a database.  A value of zero means that the timeout is the default system timeout if there is
   * one; otherwise, it means that there is no timeout. When a <code>DataSource</code> object is
   * created, the login timeout is initially zero.
   *
   * @return the data source login time limit
   * @see #setLoginTimeout
   * @since 1.4
   */
  public int getLoginTimeout() {
    if (connectTimeout != null) {
      return connectTimeout / 1000;
    }
    return (urlParser != null) ? urlParser.getOptions().connectTimeout / 1000 : 0;
  }

  /**
   * Sets the maximum time in seconds that this data source will wait while attempting to connect to
   * a database.  A value of zero specifies that the timeout is the default system timeout if there
   * is one; otherwise, it specifies that there is no timeout. When a <code>DataSource</code> object
   * is created, the login timeout is initially zero.
   *
   * @param seconds the data source login time limit
   * @throws SQLException if a database access error occurs.
   * @see #getLoginTimeout
   * @since 1.4
   */
  @Override
  public void setLoginTimeout(final int seconds) throws SQLException {
    checkNotInitialized();
    connectTimeout = seconds * 1000;
  }

  /**
   * Returns an object that implements the given interface to allow access to non-standard methods,
   * or standard methods not exposed by the proxy.
   *
   * <p>If the receiver implements the interface then the result is the receiver or a proxy for the
   * receiver. If the receiver is a wrapper and the wrapped object implements the interface then the
   * result is the wrapped object or a proxy for the wrapped object. Otherwise return the the result
   * of calling <code>unwrap</code> recursively on the wrapped object or a proxy for that result. If
   * the receiver is not a wrapper and does not implement the interface, then an
   * <code>SQLException</code> is thrown.</p>
   *
   * @param iface A Class defining an interface that the result must implement.
   * @return an object that implements the interface. May be a proxy for the actual implementing
   *     object.
   * @throws SQLException If no object found that implements the interface
   * @since 1.6
   */
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    try {
      if (isWrapperFor(iface)) {
        return iface.cast(this);
      } else {
        throw new SQLException(
            "The receiver is not a wrapper and does not implement the interface");
      }
    } catch (Exception e) {
      throw new SQLException("The receiver is not a wrapper and does not implement the interface");
    }
  }

  /**
   * Returns true if this either implements the interface argument or is directly or indirectly a
   * wrapper for an object that does. Returns false otherwise. If this implements the interface then
   * return true, else if this is a wrapper then return the result of recursively calling
   * <code>isWrapperFor</code> on the wrapped object. If this does not implement the interface and
   * is not a wrapper, return false. This method should be implemented as a low-cost operation
   * compared to <code>unwrap</code> so that callers can use this method to avoid expensive
   * <code>unwrap</code> calls that may fail. If this method returns true then calling
   * <code>unwrap</code> with the
   * same argument should succeed.
   *
   * @param interfaceOrWrapper a Class defining an interface.
   * @return true if this implements the interface or directly or indirectly wraps an object that
   *     does.
   * @throws SQLException if an error occurs while determining whether this is a wrapper for an
   *                      object with the given interface.
   * @since 1.6
   */
  public boolean isWrapperFor(final Class<?> interfaceOrWrapper) throws SQLException {
    return interfaceOrWrapper.isInstance(this);
  }

  @Override
  public XAConnection getXAConnection() throws SQLException {
    return new MariaXaConnection((MariaDbConnection) getConnection());
  }

  @Override
  public XAConnection getXAConnection(String user, String password) throws SQLException {
    return new MariaXaConnection((MariaDbConnection) getConnection(user, password));
  }

  public Logger getParentLogger() {
    return null;
  }

  /**
   * For testing purpose only.
   *
   * @return current url parser.
   */
  protected UrlParser getUrlParser() {
    return urlParser;
  }

  public String getPoolName() {
    return (pool != null) ? pool.getPoolTag() : poolName;
  }

  public void setPoolName(String poolName) throws SQLException {
    checkNotInitialized();
    this.poolName = poolName;
  }

  /**
   * Pool maximum connection size.
   *
   * @return current value.
   */
  public int getMaxPoolSize() {
    if (maxPoolSize == null) {
      return 8;
    }
    return maxPoolSize;
  }

  public void setMaxPoolSize(int maxPoolSize) throws SQLException {
    checkNotInitialized();
    this.maxPoolSize = maxPoolSize;
  }

  /**
   * Get minimum pool size (pool will grow at creation untile reaching this size).
   * Null mean use the pool maximum pool size.
   *
   * @return current value.
   */
  public int getMinPoolSize() {
    if (minPoolSize == null) {
      return getMaxPoolSize();
    }
    return minPoolSize;
  }

  public void setMinPoolSize(int minPoolSize) throws SQLException {
    checkNotInitialized();
    this.minPoolSize = minPoolSize;
  }

  /**
   * Max time a connection can be idle.
   *
   * @return current value.
   */
  public int getMaxIdleTime() {
    if (maxIdleTime == null) {
      return 600;
    }
    return maxIdleTime;
  }

  public void setMaxIdleTime(int maxIdleTime) throws SQLException {
    checkNotInitialized();
    this.maxIdleTime = maxIdleTime;
  }

  public Boolean getStaticGlobal() {
    return staticGlobal;
  }

  public void setStaticGlobal(Boolean staticGlobal) {
    this.staticGlobal = staticGlobal;
  }

  /**
   * If connection has been used in less time than poolValidMinDelay, then no connection validation
   * will be done (0=mean validation every time).
   *
   * @return current value of poolValidMinDelay
   */
  public Integer getPoolValidMinDelay() {
    if (poolValidMinDelay == null) {
      return 1000;
    }
    return poolValidMinDelay;
  }

  public void setPoolValidMinDelay(Integer poolValidMinDelay) {
    this.poolValidMinDelay = poolValidMinDelay;
  }

  private synchronized void initializeUrlParser() throws SQLException {

    if (url != null && !url.isEmpty()) {
      Properties props = new Properties();
      props.setProperty("pool", "true");
      if (user != null) {
        props.setProperty("user", user);
      }
      if (password != null) {
        props.setProperty("password", password);
      }
      if (poolName != null) {
        props.setProperty("poolName", poolName);
      }

      if (database != null) {
        props.setProperty("database", database);
      }
      if (maxPoolSize != null) {
        props.setProperty("maxPoolSize", String.valueOf(maxPoolSize));
      }
      if (minPoolSize != null) {
        props.setProperty("minPoolSize", String.valueOf(minPoolSize));
      }
      if (maxIdleTime != null) {
        props.setProperty("maxIdleTime", String.valueOf(maxIdleTime));
      }
      if (connectTimeout != null) {
        props.setProperty("connectTimeout", String.valueOf(connectTimeout));
      }
      if (staticGlobal != null) {
        props.setProperty("staticGlobal", String.valueOf(staticGlobal));
      }
      if (poolValidMinDelay != null) {
        props.setProperty("poolValidMinDelay", String.valueOf(poolValidMinDelay));
      }

      urlParser = UrlParser.parse(url, props);

    } else {

      Options options = DefaultOptions.defaultValues(HaMode.NONE);
      options.pool = true;
      options.user = user;
      options.password = password;
      options.poolName = poolName;

      if (maxPoolSize != null) {
        options.maxPoolSize = maxPoolSize;
      }
      if (minPoolSize != null) {
        options.minPoolSize = minPoolSize;
      }
      if (maxIdleTime != null) {
        options.maxIdleTime = maxIdleTime;
      }
      if (staticGlobal != null) {
        options.staticGlobal = staticGlobal;
      }
      if (connectTimeout != null) {
        options.connectTimeout = connectTimeout;
      }
      if (poolValidMinDelay != null) {
        options.poolValidMinDelay = poolValidMinDelay;
      }

      urlParser = new UrlParser(database,
          Collections.singletonList(
              new HostAddress(
                  (hostname == null || hostname.isEmpty()) ? "localhost" : hostname,
                  port == null ? 3306 : port)),
          options,
          HaMode.NONE);
    }
  }

  /**
   * Close datasource.
   */
  public void close() {
    try {
      if (pool != null) {
        pool.close();
      }
    } catch (InterruptedException interrupted) {
      //eat
    }
  }

  /**
   * Initialize pool.
   *
   * @throws SQLException if connection string has error
   */
  public synchronized void initialize() throws SQLException {
    if (pool == null) {
      initializeUrlParser();
      pool = Pools.retrievePool(urlParser);
    }
  }

  /**
   * Get current idle threads. !! For testing purpose only !!
   *
   * @return current thread id's
   */
  public List<Long> testGetConnectionIdleThreadIds() {
    return pool.testGetConnectionIdleThreadIds();
  }

  /**
   * Permit to create test that doesn't wait for maxIdleTime minimum value of 60 seconds. !! For
   * testing purpose only !!
   *
   * @param maxIdleTime forced value of maxIdleTime option.
   * @throws SQLException if connection string has error
   */
  public void testForceMaxIdleTime(int maxIdleTime) throws SQLException {
    initializeUrlParser();
    urlParser.getOptions().maxIdleTime = maxIdleTime;
    pool = Pools.retrievePool(urlParser);
  }

  /**
   * Get pool. !! For testing purpose only !!
   *
   * @return pool
   */
  public Pool testGetPool() {
    return pool;
  }

}
