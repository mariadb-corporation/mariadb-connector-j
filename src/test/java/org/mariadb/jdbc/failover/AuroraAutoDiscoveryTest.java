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

package org.mariadb.jdbc.failover;

import com.amazonaws.services.rds.model.DBInstanceNotFoundException;
import com.amazonaws.services.rds.model.InvalidDBClusterStateException;
import com.amazonaws.services.rds.model.InvalidDBInstanceStateException;
import com.amazonaws.services.rds.model.ModifyDBInstanceRequest;
import org.junit.*;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AuroraAutoDiscoveryTest extends BaseMultiHostTest {

    /**
     * Initialisation.
     */
    @BeforeClass()
    public static void beforeClass2() {
        proxyUrl = proxyAuroraUrl;
        System.out.println("environment variable \"AURORA\" value : " + System.getenv("AURORA"));
        Assume.assumeTrue(initialAuroraUrl != null && System.getenv("AURORA") != null && amazonRDSClient != null);
    }

    /**
     * Initialisation.
     */
    @Before
    public void init() {
        defaultUrl = initialAuroraUrl;
        currentType = HaMode.AURORA;
    }

    /**
     * Creates a mock replica_host_status table to imitate the database used to retrieve information about the endpoints.
     *
     * @param insertEntryQuery - Query to insert a new entry into the table before running the tests
     * @throws SQLException if unexpected error occur
     */
    private Connection tableSetup(String insertEntryQuery) throws Throwable {
        Connection connection = getNewConnection(true);
        try {
            Statement statement = connection.createStatement();
            statement.executeQuery("DROP TABLE IF EXISTS replica_host_status");
            statement.executeQuery("CREATE TABLE replica_host_status (SERVER_ID VARCHAR(255), SESSION_ID VARCHAR(255), "
                    + "LAST_UPDATE_TIMESTAMP TIMESTAMP DEFAULT NOW())");

            ResultSet resultSet = statement.executeQuery("SELECT SERVER_ID, SESSION_ID, LAST_UPDATE_TIMESTAMP "
                    + "FROM information_schema.replica_host_status "
                    + "WHERE LAST_UPDATE_TIMESTAMP = ("
                    + "SELECT MAX(LAST_UPDATE_TIMESTAMP) "
                    + "FROM information_schema.replica_host_status)");

            while (resultSet.next()) {
                StringBuilder values = new StringBuilder();
                for (int i = 1; i < 4; i++) {
                    values.append((i == 1) ? "'localhost'" : ",'" + resultSet.getString(i) + "'");
                }
                statement.executeQuery("INSERT INTO replica_host_status (SERVER_ID, SESSION_ID, LAST_UPDATE_TIMESTAMP) "
                        + "VALUES (" + values + ")");
            }

            if (insertEntryQuery != null) {
                statement.executeQuery(insertEntryQuery);
            }

            int serverId = getServerId(connection);
            stopProxy(serverId, 1);
            Statement statement2 = connection.createStatement();
            statement2.executeQuery("select 1");
            statement2.close();

        } catch (SQLException se) {
            fail("Unable to execute queries to set up table: " + se);
        }

        return connection;
    }

    /**
     * Takes down the table created solely for these tests.
     *
     * @throws SQLException if unexpected error occur
     */
    @After
    public void after() throws SQLException {
        Connection connection = null;
        try {
            connection = getNewConnection(true);
            Statement statement = connection.createStatement();
            statement.executeQuery("DROP TABLE IF EXISTS replica_host_status");
        } finally {
            if (connection != null) connection.close();
        }
    }

    /**
     * Test verifies that the driver discovers new instances as soon as they are available.
     *
     * @throws Throwable if unexpected error occur
     */
    @Test
    public void testDiscoverCreatedInstanceOnFailover() throws Throwable {
        Connection connection = null;
        try {
            connection = tableSetup(null);
            int masterServerId = getServerId(connection);
            final int initialSize = getProtocolFromConnection(connection).getUrlParser().getHostAddresses().size();

            Statement statement = connection.createStatement();
            statement.executeQuery("INSERT INTO replica_host_status (SERVER_ID, SESSION_ID) "
                    + "VALUES ('test-discovery-on-creation', 'mock-new-endpoint')");

            stopProxy(masterServerId, 1);
            statement.executeQuery("select 1");

            List<HostAddress> finalEndpoints = getProtocolFromConnection(connection).getUrlParser().getHostAddresses();
            boolean newEndpointFound = foundHostInList(finalEndpoints, "test-discovery-on-creation");

            assertTrue("Discovered new endpoint on failover", newEndpointFound);
            assertEquals(initialSize + 1, finalEndpoints.size());
        } finally {
            if (connection != null) connection.close();
        }
    }

    /**
     * Test verifies that deleted instances are removed from the possible connections.
     *
     * @throws Throwable if unexpected error occur
     */
    @Test
    public void testRemoveDeletedInstanceOnFailover() throws Throwable {
        Connection connection = null;
        try {
            connection = tableSetup("INSERT INTO replica_host_status (SERVER_ID, SESSION_ID) "
                    + "VALUES ('test-instance-deleted-detection', 'mock-delete-endpoint')");
            Protocol protocol = getProtocolFromConnection(connection);
            final int initialSize = protocol.getUrlParser().getHostAddresses().size();
            int serverId = getServerId(connection);

            Statement statement = connection.createStatement();
            statement.executeQuery("UPDATE replica_host_status "
                    + "SET LAST_UPDATE_TIMESTAMP = DATE_SUB(LAST_UPDATE_TIMESTAMP, INTERVAL 4 MINUTE) "
                    + "WHERE SERVER_ID = 'test-instance-deleted-detection'");
            stopProxy(serverId, 1);
            statement.executeQuery("select 1");

            List<HostAddress> finalEndpoints = protocol.getUrlParser().getHostAddresses();
            boolean deletedInstanceGone = !foundHostInList(finalEndpoints, "test-instance-deleted-detection");

            assertTrue("Removed deleted endpoint from urlParser", deletedInstanceGone);
            assertEquals(initialSize - 1, finalEndpoints.size());
        } finally {
            if (connection != null) connection.close();
        }
    }

    /**
     * Must set newlyCreatedInstance system property in which the instance is not the current writer.
     * The best way to test is to create a new instance as the test is started.
     * All other instances should have a promotion tier greater than zero.
     * Test checks if a newly created instance that is promoted as the writer is found and connected to right away.
     *
     * @throws Throwable if error occur
     */
    @Test
    public void testNewInstanceAsWriterDetection() throws Throwable {
        Assume.assumeTrue("System property newlyCreatedInstance is set", System.getProperty("newlyCreatedInstance") != null);
        Connection connection = null;
        try {
            connection = getNewConnection(false);
            final String initialHost = getProtocolFromConnection(connection).getHost();

            ModifyDBInstanceRequest request1 = new ModifyDBInstanceRequest();
            request1.setDBInstanceIdentifier(System.getProperty("newlyCreatedInstance"));
            request1.setPromotionTier(0);

            boolean promotionTierChanged;
            do {
                try {
                    amazonRDSClient.modifyDBInstance(request1);
                    promotionTierChanged = true;
                } catch (InvalidDBInstanceStateException e) {
                    promotionTierChanged = false;
                } catch ( DBInstanceNotFoundException e) {
                    promotionTierChanged = false;
                }
            } while (!promotionTierChanged);

            try {
                Thread.sleep(10 * 1000); // Should have completed modification
            } catch (InterruptedException e) {
                fail("Thread sleep was interrupted");
            }

            launchAuroraFailover();
            try {
                Thread.sleep(30 * 1000); // Should have failed over
            } catch (InterruptedException e) {
                fail("Thread sleep was interrupted");
            }

            Statement statement = connection.createStatement();
            statement.executeQuery("select 1");

            String newHost = getProtocolFromConnection(connection).getHost();
            assertFalse("Connected to new writer", initialHost.equals(newHost));
            assertEquals(System.getProperty("newlyCreatedInstance"), newHost.substring(0, newHost.indexOf(".")));
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void testExceptionHandlingWhenDataFromTable() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection(false);
            final String initialHost = getProtocolFromConnection(connection).getHost();

            final Statement statement = connection.createStatement();
            Thread queryThread = new Thread(() -> {
                long startTime = System.nanoTime();
                long stopTime = System.nanoTime();
                try {
                    while (Math.abs(TimeUnit.NANOSECONDS.toMillis(stopTime - startTime)) < 1000) {
                        stopTime = System.nanoTime();
                        statement.executeQuery("SELECT 1");
                        startTime = System.nanoTime();
                    }
                } catch (SQLException se) {
                    se.printStackTrace();
                }
            });

            Thread failoverThread = new Thread() {
                public void run() {
                    do {
                        try {
                            launchAuroraFailover();
                        } catch (InvalidDBClusterStateException e) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException ie) {
                                // Expected because may occur due to queryThread
                            }
                        }
                    } while (!isInterrupted());
                }
            };

            queryThread.start();
            failoverThread.start();
            queryThread.join();
            if (!queryThread.isAlive()) {
                failoverThread.interrupt();
            }

            if (statement != null) {
                statement.close();
            }

            Set<HostAddress> hostAddresses = getProtocolFromConnection(connection).getProxy().getListener().getBlacklistKeys();
            boolean connectionBlacklisted = foundHostInList(hostAddresses, initialHost);
            assertTrue("Connection has been blacklisted", connectionBlacklisted);
        } finally {
            if (connection != null) connection.close();
        }
    }

    private boolean foundHostInList(Collection<HostAddress> hostAddresses, String hostIdentifier) {
        for (HostAddress hostAddress : hostAddresses) {
            if (hostAddress.host.contains(hostIdentifier)) {
                return true;
            }
        }
        return false;
    }


    /**
     * CONJ-392 : aurora must discover active nodes without timezone issue.
     *
     * @throws Throwable if error occur
     */
    @Test
    public void testTimeZoneDiscovery() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&sessionVariables=@@time_zone='US/Central'", false);
            List<HostAddress> hostAddresses = getProtocolFromConnection(connection).getProxy().getListener().getUrlParser().getHostAddresses();
            for (HostAddress hostAddress : hostAddresses) {
                System.out.println("hostAddress:" + hostAddress);
            }
            assertTrue(hostAddresses.size() > 1);
        } finally {
            if (connection != null) connection.close();
        }
    }
}
