package org.mariadb.jdbc.failover;

import com.amazonaws.services.rds.model.DBInstanceNotFoundException;
import com.amazonaws.services.rds.model.InvalidDBClusterStateException;
import com.amazonaws.services.rds.model.InvalidDBInstanceStateException;
import com.amazonaws.services.rds.model.ModifyDBInstanceRequest;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void beforeClass2() throws SQLException {
        proxyUrl = proxyAuroraUrl;
        System.out.println("environment variable \"AURORA\" value : " + System.getenv("AURORA"));
        Assume.assumeTrue(initialAuroraUrl != null && System.getenv("AURORA") != null && amazonRDSClient != null);
    }

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @Before
    public void init() throws SQLException {
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
        try (Statement statement = connection.createStatement()) {
            statement.executeQuery("DROP TABLE IF EXISTS replica_host_status");
            statement.executeQuery("CREATE TABLE replica_host_status (SERVER_ID VARCHAR(255), SESSION_ID VARCHAR(255), "
                    + "LAST_UPDATE_TIMESTAMP TIMESTAMP DEFAULT NOW())");

            ResultSet resultSet = statement.executeQuery("SELECT SERVER_ID, SESSION_ID, LAST_UPDATE_TIMESTAMP "
                    + "FROM information_schema.replica_host_status "
                    + "WHERE LAST_UPDATE_TIMESTAMP = ("
                    + "SELECT MAX(LAST_UPDATE_TIMESTAMP) "
                    + "FROM information_schema.replica_host_status)");

            while (resultSet.next()) {
                String values = "";
                for (int i = 1; i < 4; i++) {
                    values += (i == 1) ? "'localhost'" : ",'" + resultSet.getString(i) + "'";
                }
                statement.executeQuery("INSERT INTO replica_host_status (SERVER_ID, SESSION_ID, LAST_UPDATE_TIMESTAMP) "
                        + "VALUES (" + values + ")");
            }

            if (insertEntryQuery != null) {
                statement.executeQuery(insertEntryQuery);
            }

            try {
                setDbName(connection, "testj");
            } catch (Throwable t) {
                fail("Unable to set database for testing");
            }

            int serverId = getServerId(connection);
            stopProxy(serverId, 1);
            try (Statement statement2 = connection.createStatement()) {
                statement2.executeQuery("select 1");
            }

        } catch (SQLException se) {
            fail("Unable to execute queries to set up table: " + se);
        }

        return connection;
    }

    /**
     * Takes down the table created solely for these tests.
     *
     * @throws SQLException  if unexpected error occur
     */
    @After
    public void after() throws SQLException {
        try (Connection connection = getNewConnection(true)) {
            try (Statement statement = connection.createStatement()) {
                statement.executeQuery("DROP TABLE IF EXISTS replica_host_status");
            }
        }
    }

    /**
     * Test verifies that the driver discovers new instances as soon as they are available.
     *
     * @throws Throwable if unexpected error occur
     */
    @Test
    public void testDiscoverCreatedInstanceOnFailover() throws Throwable {

        try (Connection connection = tableSetup(null)) {
            int masterServerId = getServerId(connection);
            final int initialSize = getProtocolFromConnection(connection).getUrlParser().getHostAddresses().size();

            try (Statement statement = connection.createStatement()) {
                statement.executeQuery("INSERT INTO replica_host_status (SERVER_ID, SESSION_ID) "
                        + "VALUES ('test-discovery-on-creation', 'mock-new-endpoint')");

                stopProxy(masterServerId, 1);
                statement.executeQuery("select 1");

                List<HostAddress> finalEndpoints = getProtocolFromConnection(connection).getUrlParser().getHostAddresses();
                boolean newEndpointFound = foundHostInList(finalEndpoints, "test-discovery-on-creation");

                assertTrue("Discovered new endpoint on failover", newEndpointFound);
                assertEquals(initialSize + 1, finalEndpoints.size());
            }
        }
    }

    /**
     * Test verifies that deleted instances are removed from the possible connections.
     *
     * @throws Throwable if unexpected error occur
     */
    @Test
    public void testRemoveDeletedInstanceOnFailover() throws Throwable {
        try (Connection connection = tableSetup("INSERT INTO replica_host_status (SERVER_ID, SESSION_ID) "
                + "VALUES ('test-instance-deleted-detection', 'mock-delete-endpoint')")) {
            Protocol protocol = getProtocolFromConnection(connection);
            final int initialSize = protocol.getUrlParser().getHostAddresses().size();
            int serverId = getServerId(connection);

            try (Statement statement = connection.createStatement()) {
                statement.executeQuery("UPDATE replica_host_status "
                        + "SET LAST_UPDATE_TIMESTAMP = DATE_SUB(LAST_UPDATE_TIMESTAMP, INTERVAL 4 MINUTE) "
                        + "WHERE SERVER_ID = 'test-instance-deleted-detection'");
                stopProxy(serverId, 1);
                statement.executeQuery("select 1");
            }

            List<HostAddress> finalEndpoints = protocol.getUrlParser().getHostAddresses();
            boolean deletedInstanceGone = !foundHostInList(finalEndpoints, "test-instance-deleted-detection");

            assertTrue("Removed deleted endpoint from urlParser", deletedInstanceGone);
            assertEquals(initialSize - 1, finalEndpoints.size());
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

        try (Connection connection = getNewConnection(false)) {
            final String initialHost = getProtocolFromConnection(connection).getHost();

            ModifyDBInstanceRequest request1 = new ModifyDBInstanceRequest();
            request1.setDBInstanceIdentifier(System.getProperty("newlyCreatedInstance"));
            request1.setPromotionTier(0);

            boolean promotionTierChanged;
            do {
                try {
                    amazonRDSClient.modifyDBInstance(request1);
                    promotionTierChanged = true;
                } catch (InvalidDBInstanceStateException | DBInstanceNotFoundException e) {
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

            try (Statement statement = connection.createStatement()) {
                statement.executeQuery("select 1");
            }

            String newHost = getProtocolFromConnection(connection).getHost();
            assertTrue("Connected to new writer", !initialHost.equals(newHost));
            assertEquals(System.getProperty("newlyCreatedInstance"), newHost.substring(0, newHost.indexOf(".")));
        }
    }

    @Test
    public void testExceptionHandlingWhenDataFromTable() throws Throwable {
        try (Connection connection = getNewConnection(false)) {
            final String initialHost = getProtocolFromConnection(connection).getHost();

            final Statement statement = connection.createStatement();
            Thread queryThread = new Thread() {
                public void run() {
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
                }
            };

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
        }
    }

    private boolean foundHostInList(Collection<HostAddress> hostAddresses, String hostIdentifier) {
        for (HostAddress hostAddress : hostAddresses) {
            if (hostAddress.host.indexOf(hostIdentifier) > -1) {
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

        try (Connection connection = getNewConnection("&sessionVariables=@@time_zone='US/Central'",false)) {
            List<HostAddress> hostAddresses = getProtocolFromConnection(connection).getProxy().getListener().getUrlParser().getHostAddresses();
            for (HostAddress hostAddress : hostAddresses) {
                System.out.println("hostAddress:" + hostAddress);
            }
            assertTrue(hostAddresses.size() > 1);
        }
    }
}
