// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc;

import com.singlestore.jdbc.client.Client;
import com.singlestore.jdbc.client.impl.MultiPrimaryClient;
import com.singlestore.jdbc.client.impl.MultiPrimaryReplicaClient;
import com.singlestore.jdbc.client.impl.ReplayClient;
import com.singlestore.jdbc.client.impl.StandardClient;
import com.singlestore.jdbc.pool.Pools;
import com.singlestore.jdbc.util.VersionFactory;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

public final class Driver implements java.sql.Driver {

  static {
    try {
      DriverManager.registerDriver(new Driver());
    } catch (SQLException e) {
      // eat
    }
  }

  /**
   * Connect according to configuration
   *
   * @param configuration configuration
   * @return a Connection
   * @throws SQLException if connect fails
   */
  public static Connection connect(Configuration configuration) throws SQLException {
    ReentrantLock lock = new ReentrantLock();
    Client client;
    switch (configuration.haMode()) {
      case LOADBALANCE:
      case SEQUENTIAL:
        client = new MultiPrimaryClient(configuration, lock);
        break;

      case REPLICATION:
        // additional check
        client = new MultiPrimaryReplicaClient(configuration, lock);
        break;

      default:
        ClientInstance<Configuration, HostAddress, ReentrantLock, Boolean, Client> clientInstance =
            (configuration.transactionReplay()) ? ReplayClient::new : StandardClient::new;

        if (configuration.addresses().isEmpty()) {
          // unix socket / windows pipe
          client = clientInstance.apply(configuration, null, lock, false);
        } else {
          // loop until finding
          SQLException lastException = null;
          for (HostAddress host : configuration.addresses()) {
            try {
              client = clientInstance.apply(configuration, host, lock, false);
              return new Connection(configuration, lock, client);
            } catch (SQLException e) {
              lastException = e;
            }
          }
          throw lastException;
        }
        break;
    }
    return new Connection(configuration, lock, client);
  }

  @FunctionalInterface
  private interface ClientInstance<T, U, V, W, R> {
    R apply(T t, U u, V v, W w) throws SQLException;
  }

  /**
   * Connect to the given connection string.
   *
   * @param url the url to connect to
   * @return a connection
   * @throws SQLException if it is not possible to connect
   */
  public Connection connect(final String url, final Properties props) throws SQLException {
    Configuration configuration = Configuration.parse(url, props);
    if (configuration != null) {
      if (configuration.pool()) {
        return Pools.retrievePool(configuration).getPoolConnection().getConnection();
      }
      return connect(configuration);
    }
    return null;
  }

  /**
   * returns true if the driver can accept the url.
   *
   * @param url the url to test
   * @return true if the url is valid for this driver
   */
  @Override
  public boolean acceptsURL(String url) {
    return Configuration.acceptsUrl(url);
  }

  /**
   * Get the property info.
   *
   * @param url the url to get properties for
   * @param info the info props
   * @return all possible connector options
   * @throws SQLException if there is a problem getting the property info
   */
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    Configuration conf = Configuration.parse(url, info);
    if (conf == null) {
      return new DriverPropertyInfo[0];
    }

    Properties propDesc = new Properties();
    try (InputStream inputStream =
        Driver.class.getClassLoader().getResourceAsStream("driver.properties")) {
      propDesc.load(inputStream);
    } catch (IOException io) {
      // eat
    }

    List<DriverPropertyInfo> props = new ArrayList<>();
    for (Field field : Configuration.Builder.class.getDeclaredFields()) {
      if (!field.getName().startsWith("_")) {
        try {
          Field fieldConf = Configuration.class.getDeclaredField(field.getName());
          fieldConf.setAccessible(true);
          Object obj = fieldConf.get(conf);
          String value = obj == null ? null : obj.toString();
          DriverPropertyInfo propertyInfo = new DriverPropertyInfo(field.getName(), value);
          propertyInfo.description = value == null ? "" : (String) propDesc.get(field.getName());
          propertyInfo.required = false;
          props.add(propertyInfo);
        } catch (IllegalAccessException | NoSuchFieldException e) {
          // eat error
        }
      }
    }
    return props.toArray(new DriverPropertyInfo[0]);
  }

  /**
   * gets the major version of the driver.
   *
   * @return the major versions
   */
  public int getMajorVersion() {
    return VersionFactory.getInstance().getMajorVersion();
  }

  /**
   * gets the minor version of the driver.
   *
   * @return the minor version
   */
  public int getMinorVersion() {
    return VersionFactory.getInstance().getMinorVersion();
  }

  /**
   * checks if the driver is jdbc compliant.
   *
   * @return true since the driver is not compliant
   */
  public boolean jdbcCompliant() {
    return true;
  }

  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("Use logging parameters for enabling logging.");
  }
}
