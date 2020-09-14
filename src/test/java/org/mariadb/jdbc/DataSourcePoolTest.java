/*
 *
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

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataSourcePoolTest extends BaseTest {

  protected static final String defConnectToIP = null;
  protected static String connectToIP;

  /** Initialisation. */
  @BeforeClass
  public static void beforeClassDataSourceTest() {
    Assume.assumeTrue(System.getenv("SKYSQL") == null);
    connectToIP = System.getProperty("testConnectToIP", defConnectToIP);
  }

  @Test
  public void testDataSource() throws SQLException {
    Assume.assumeFalse(options.useSsl != null && options.useSsl);

    try (MariaDbPoolDataSource ds =
        new MariaDbPoolDataSource(hostname == null ? "localhost" : hostname, port, database)) {
      try (Connection connection = ds.getConnection(username, password)) {
        assertEquals(connection.isValid(0), true);
      }
    }
  }

  @Test
  public void testDataSource2() throws SQLException {
    Assume.assumeFalse(options.useSsl != null && options.useSsl);
    try (MariaDbPoolDataSource ds =
        new MariaDbPoolDataSource(hostname == null ? "localhost" : hostname, port, database)) {
      try (Connection connection = ds.getConnection(username, password)) {
        assertEquals(connection.isValid(0), true);
      }
    }
  }

  @Test
  public void testDataSourceEmpty() throws SQLException {
    Assume.assumeFalse(options.useSsl != null && options.useSsl);
    try (MariaDbPoolDataSource ds = new MariaDbPoolDataSource()) {
      ds.setDatabaseName(database);
      ds.setPort(port);
      ds.setServerName(hostname == null ? "localhost" : hostname);
      try (Connection connection = ds.getConnection(username, password)) {
        assertEquals(connection.isValid(0), true);
      }
    }
  }

  @Test
  public void testDataSourcePool() throws SQLException {
    try (MariaDbPoolDataSource ds = new MariaDbPoolDataSource()) {
      assertEquals(ds.getPort(), 3306);
      assertEquals(ds.getMaxIdleTime(), 600);
      assertEquals(ds.getMaxPoolSize(), 8);
      assertEquals(ds.getMinPoolSize(), 8);
      assertEquals(ds.getPoolValidMinDelay(), Integer.valueOf(1000));

      ds.setPort(33006);
      ds.setMaxPoolSize(10);
      ds.setMaxIdleTime(500);
      ds.setPoolValidMinDelay(500);

      assertEquals(ds.getMaxIdleTime(), 500);
      assertEquals(ds.getMaxPoolSize(), 10);
      assertEquals(ds.getMinPoolSize(), 10);
      assertEquals(ds.getPort(), 33006);
      assertEquals(ds.getPoolValidMinDelay(), Integer.valueOf(500));

      ds.setMinPoolSize(5);

      assertEquals(ds.getMaxPoolSize(), 10);
      assertEquals(ds.getMinPoolSize(), 5);
    }
  }

  /**
   * Conj-80.
   *
   * @throws SQLException exception
   */
  @Test
  public void setDatabaseNameTest() throws SQLException {
    Assume.assumeTrue(System.getenv("SKYSQL") == null);
    try (MariaDbPoolDataSource ds =
        new MariaDbPoolDataSource(hostname == null ? "localhost" : hostname, port, database)) {
      try (Connection connection = ds.getConnection(username, password)) {
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test2");
        try {
          ds.setDatabaseName("test2");
          fail();
        } catch (SQLException sqle) {
          assertTrue(
              sqle.getMessage()
                  .contains("can not perform a configuration change once initialized"));
        }
      }
    }
  }

  /**
   * Conj-80.
   *
   * @throws SQLException exception
   */
  @Test
  public void setServerNameTest() throws SQLException {
    Assume.assumeTrue(connectToIP != null);
    try (MariaDbPoolDataSource ds =
        new MariaDbPoolDataSource(hostname == null ? "localhost" : hostname, port, database)) {
      try (Connection connection = ds.getConnection(username, password)) {
        try {
          ds.setServerName(connectToIP);
          fail();
        } catch (SQLException sqle) {
          assertTrue(
              sqle.getMessage()
                  .contains("can not perform a configuration change once initialized"));
        }
      }
    }
  }

  /**
   * Conj-80.
   *
   * @throws SQLException exception
   */
  @Test(timeout = 20000) // unless port 3307 can be used
  public void setPortTest() throws SQLException {
    Assume.assumeFalse("true".equals(System.getenv("AURORA")));
    Assume.assumeFalse(options.useSsl != null && options.useSsl);

    try (MariaDbPoolDataSource ds =
        new MariaDbPoolDataSource(hostname == null ? "localhost" : hostname, port, database)) {
      try (Connection connection2 = ds.getConnection(username, password)) {
        // delete blacklist, because can failover on 3306 is filled
        assureBlackList(connection2);
        connection2.close();
      }

      try {
        ds.setPort(3407);
        fail("is initialized, must throw an exception");
      } catch (SQLException sqle) {
        assertTrue(
            sqle.getMessage().contains("can not perform a configuration change once initialized"));
      }

      // must throw SQLException
      try (Connection connection = ds.getConnection(username, password)) {
        // do nothing
      } catch (SQLException e) {
        fail();
      }
    }
  }

  /**
   * Conj-123:Session variables lost and exception if set via
   * MariaDbPoolDataSource.setProperties/setURL.
   *
   * @throws SQLException exception
   */
  @Test
  public void setPropertiesTest() throws SQLException {
    try (MariaDbPoolDataSource ds =
        new MariaDbPoolDataSource(hostname == null ? "localhost" : hostname, port, database)) {
      ds.setUrl(connUri + "&sessionVariables=sql_mode='PIPES_AS_CONCAT'");
      try (Connection connection = ds.getConnection(username, password)) {
        ResultSet rs = connection.createStatement().executeQuery("SELECT @@sql_mode");
        if (rs.next()) {
          assertEquals("PIPES_AS_CONCAT", rs.getString(1));
          try {
            ds.setUrl(connUri + "&sessionVariables=sql_mode='ALLOW_INVALID_DATES'");
            fail();
          } catch (SQLException sqlException) {
            assertTrue(
                sqlException
                    .getMessage()
                    .contains("can not perform a configuration change once initialized"));
          }
          try (Connection connection2 = ds.getConnection()) {
            rs = connection2.createStatement().executeQuery("SELECT @@sql_mode");
            assertTrue(rs.next());
            assertEquals("PIPES_AS_CONCAT", rs.getString(1));
          }
        } else {
          fail();
        }
      }
    }
  }

  @Test
  public void setLoginTimeOut() throws SQLException {
    try (MariaDbPoolDataSource ds =
        new MariaDbPoolDataSource(hostname == null ? "localhost" : hostname, port, database)) {
      assertEquals(0, ds.getLoginTimeout());
      ds.setLoginTimeout(10);
      assertEquals(10, ds.getLoginTimeout());
    }
  }
}
