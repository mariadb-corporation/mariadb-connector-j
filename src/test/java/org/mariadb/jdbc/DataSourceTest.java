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
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class DataSourceTest extends BaseTest {
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
        MariaDbDataSource ds = new MariaDbDataSource(hostname == null ? "localhost" : hostname, port, database);

        try (Connection connection = ds.getConnection(username, password)) {
            assertEquals(connection.isValid(0), true);
        }
    }

    @Test
    public void testDataSource2() throws SQLException {
        MariaDbDataSource ds = new MariaDbDataSource(hostname == null ? "localhost" : hostname, port, database);
        try (Connection connection = ds.getConnection(username, password)) {
            assertEquals(connection.isValid(0), true);
        }
    }

    @Test
    public void testDataSourceEmpty() throws SQLException {
        MariaDbDataSource ds = new MariaDbDataSource();
        ds.setDatabaseName(database);
        ds.setPort(port);
        ds.setServerName(hostname == null ? "localhost" : hostname);
        try (Connection connection = ds.getConnection(username, password)) {
            assertEquals(connection.isValid(0), true);
        }
    }

    /**
     * Conj-80.
     *
     * @throws SQLException exception
     */
    @Test
    public void setDatabaseNameTest() throws SQLException {
        MariaDbDataSource ds = new MariaDbDataSource(hostname == null ? "localhost" : hostname, port, database);
        try (Connection connection = ds.getConnection(username, password)) {
            connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test2");
            ds.setDatabaseName("test2");

            try (Connection connection2 = ds.getConnection(username, password)) {
                assertEquals("test2", ds.getDatabaseName());
                assertEquals(ds.getDatabaseName(), connection2.getCatalog());
                connection2.createStatement().execute("DROP DATABASE IF EXISTS test2");
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
        MariaDbDataSource ds = new MariaDbDataSource(hostname == null ? "localhost" : hostname, port, database);
        try (Connection connection = ds.getConnection(username, password)) {
            ds.setServerName(connectToIP);
            try (Connection connection2 = ds.getConnection(username, password)) {
                //do nothing
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
        MariaDbDataSource ds = new MariaDbDataSource(hostname == null ? "localhost" : hostname, port, database);
        try (Connection connection2 = ds.getConnection(username, password)) {
            //delete blacklist, because can failover on 3306 is filled
            assureBlackList(connection2);
            connection2.close();
        }

        ds.setPort(3407);

        //must throw SQLException
        try {
            ds.getConnection(username, password);
            Assert.fail();
        } catch (SQLException e) {
            //normal error
        }
    }

    /**
     * Conj-123:Session variables lost and exception if set via MariaDbDataSource.setProperties/setURL.
     *
     * @throws SQLException exception
     */
    @Test
    public void setPropertiesTest() throws SQLException {
        MariaDbDataSource ds = new MariaDbDataSource(hostname == null ? "localhost" : hostname, port, database);
        ds.setProperties("sessionVariables=sql_mode='PIPES_AS_CONCAT'");
        try (Connection connection = ds.getConnection(username, password)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT @@sql_mode");
            if (rs.next()) {
                assertEquals("PIPES_AS_CONCAT", rs.getString(1));
                ds.setUrl(connUri + "&sessionVariables=sql_mode='ALLOW_INVALID_DATES'");
                try (Connection connection2 = ds.getConnection()) {
                    rs = connection2.createStatement().executeQuery("SELECT @@sql_mode");
                    assertTrue(rs.next());
                    assertEquals("ALLOW_INVALID_DATES", rs.getString(1));
                }
            } else {
                fail();
            }
        }

    }

    @Test
    public void setLoginTimeOut() throws SQLException {
        MariaDbDataSource ds = new MariaDbDataSource(hostname == null ? "localhost" : hostname, port, database);
        assertEquals(0, ds.getLoginTimeout());
        ds.setLoginTimeout(10);
        assertEquals(10, ds.getLoginTimeout());
    }

}
