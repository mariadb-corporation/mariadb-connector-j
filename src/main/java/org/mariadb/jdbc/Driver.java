/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
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
import org.mariadb.jdbc.client.Client;
import org.mariadb.jdbc.client.ClientImpl;
import org.mariadb.jdbc.client.MultiPrimaryClient;
import org.mariadb.jdbc.client.MultiPrimaryReplicaClient;
import org.mariadb.jdbc.util.Version;

public final class Driver implements java.sql.Driver {

  static {
    try {
      DriverManager.registerDriver(new Driver());
    } catch (SQLException e) {
      // eat
    }
  }

  protected static Connection connect(Configuration configuration) throws SQLException {
    HostAddress hostAddress = null;
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
        hostAddress = configuration.addresses().get(0);
        client = new ClientImpl(configuration, hostAddress, false, lock, false);
        break;
    }
    return new Connection(configuration, lock, client);
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
    return connect(configuration);
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
    if (url != null && !url.isEmpty()) {
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
    return new DriverPropertyInfo[0];
  }

  /**
   * gets the major version of the driver.
   *
   * @return the major versions
   */
  public int getMajorVersion() {
    return Version.majorVersion;
  }

  /**
   * gets the minor version of the driver.
   *
   * @return the minor version
   */
  public int getMinorVersion() {
    return Version.minorVersion;
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
