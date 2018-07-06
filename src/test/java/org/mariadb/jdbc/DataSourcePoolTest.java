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

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class DataSourcePoolTest extends BaseTest {
    protected static final String defConnectToIP = null;
    protected static String connectToIP;

    /**
     * Initialisation.
     */
    @BeforeClass
    public static void beforeClassDataSourceTest() {
        connectToIP = System.getProperty("testConnectToIP", defConnectToIP);
    }

    @Test
    public void testDataSource() throws SQLException {
        MariaDbPoolDataSource ds = new MariaDbPoolDataSource(hostname == null ? "localhost" : hostname, port, database);
        Connection connection = null;
        try {
            connection = ds.getConnection(username, password);
            assertEquals(connection.isValid(0), true);
        } finally {
            if (connection != null) connection.close();
        }
        ds.close();
    }

    @Test
    public void testDataSourceEmpty() throws SQLException {
        MariaDbPoolDataSource ds = new MariaDbPoolDataSource();
        ds.setDatabaseName(database);
        ds.setPort(port);
        ds.setServerName(hostname == null ? "localhost" : hostname);
        Connection connection = null;
        try {
            connection = ds.getConnection(username, password);
            assertEquals(connection.isValid(0), true);
        } finally {
            if (connection != null) connection.close();
        }
        ds.close();
    }


    @Test
    public void testDataSourcePool() throws SQLException {
      MariaDbPoolDataSource ds = null;
      try {
        ds = new MariaDbPoolDataSource();
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
      } finally {
        if (ds != null) ds.close();
      }
    }

    /**
     * Conj-80.
     *
     * @throws SQLException exception
     */
    @Test
    public void setDatabaseNameTest() throws SQLException {
        Assume.assumeTrue(System.getenv("MAXSCALE_VERSION") == null);
        MariaDbPoolDataSource ds = new MariaDbPoolDataSource(hostname == null ? "localhost" : hostname, port, database);
        Connection connection = null;
        try {
            connection = ds.getConnection(username, password);
            connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test2");
            try {
                ds.setDatabaseName("test2");
                fail();
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains(
                        "can not perform a configuration change once initialized"));
            }
        } finally {
            if (connection != null) connection.close();
        }
        ds.close();
    }

    /**
     * Conj-80.
     *
     * @throws SQLException exception
     */
    @Test
    public void setServerNameTest() throws SQLException {
        Assume.assumeTrue(connectToIP != null);
        MariaDbPoolDataSource ds = new MariaDbPoolDataSource(hostname == null ? "localhost" : hostname, port, database);
        Connection connection = null;
        try {
            connection = ds.getConnection(username, password);
            try {
                ds.setServerName(connectToIP);
                fail();
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().contains(
                        "can not perform a configuration change once initialized"));
            }
        } finally {
            if (connection != null) connection.close();
        }
        ds.close();
    }

    /**
     * Conj-80.
     *
     * @throws SQLException exception
     */
    @Test(timeout = 20000) // unless port 3307 can be used
    public void setPortTest() throws SQLException {
        Assume.assumeFalse("true".equals(System.getenv("AURORA")));
        MariaDbPoolDataSource ds = new MariaDbPoolDataSource(hostname == null ? "localhost" : hostname, port, database);

        Connection connection2 = null;
        try {
            connection2 = ds.getConnection(username, password);
            //delete blacklist, because can failover on 3306 is filled
            assureBlackList(connection2);
            connection2.close();
        } finally {
            if (connection2 != null) connection2.close();
        }

        try {
            ds.setPort(3407);
            fail("is initialized, must throw an exception");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("can not perform a configuration change once initialized"));
        }

        //must throw SQLException
        Connection connection = null;
        try {
            connection = ds.getConnection(username, password);
            //do nothing
        } catch (SQLException e) {
            fail();
        } finally {
            if (connection != null) connection.close();
        }
        ds.close();
    }

    /**
     * Conj-123:Session variables lost and exception if set via MariaDbPoolDataSource.setProperties/setURL.
     *
     * @throws SQLException exception
     */
    @Test
    public void setPropertiesTest() throws SQLException {
        MariaDbPoolDataSource ds = new MariaDbPoolDataSource(hostname == null ? "localhost" : hostname, port, database);
        ds.setUrl(connUri + "&sessionVariables=sql_mode='PIPES_AS_CONCAT'");

        Connection connection = null;
        try {
            connection = ds.getConnection(username, password);
            ResultSet rs = connection.createStatement().executeQuery("SELECT @@sql_mode");
            if (rs.next()) {
                assertEquals("PIPES_AS_CONCAT", rs.getString(1));
                try {
                    ds.setUrl(connUri + "&sessionVariables=sql_mode='ALLOW_INVALID_DATES'");
                    fail();
                } catch (SQLException sqlException) {
                    assertTrue(sqlException.getMessage().contains(
                            "can not perform a configuration change once initialized"));
                }
                Connection connection2 = null;
                try {
                    connection2 = ds.getConnection();
                    rs = connection2.createStatement().executeQuery("SELECT @@sql_mode");
                    assertTrue(rs.next());
                    assertEquals("PIPES_AS_CONCAT", rs.getString(1));
                } finally {
                    if (connection2 != null) connection2.close();
                }
            } else {
                fail();
            }
        } finally {
            if (connection != null) connection.close();
        }
        ds.close();
    }

    @Test
    public void setLoginTimeOut() throws SQLException {
        MariaDbPoolDataSource ds = new MariaDbPoolDataSource(hostname == null ? "localhost" : hostname, port, database);
        assertEquals(0, ds.getLoginTimeout());
        ds.setLoginTimeout(10);
        assertEquals(10, ds.getLoginTimeout());
        ds.close();
    }

}
