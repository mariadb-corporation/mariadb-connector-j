package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AuroraFailoverTest extends BaseReplication {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void beforeClass2() throws SQLException {
        proxyUrl = proxyAuroraUrl;
        Assume.assumeTrue(initialAuroraUrl != null);
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


    @Test
    public void testErrorWriteOnReplica() throws SQLException {
        Connection connection = null;
        try {
            connection = getNewConnection(false);
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists auroraDelete" + jobId);
            stmt.execute("create table auroraDelete" + jobId + " (id int not null primary key auto_increment, test VARCHAR(10))");
            connection.setReadOnly(true);
            Assert.assertTrue(connection.isReadOnly());
            try {
                stmt.execute("drop table if exists auroraDelete" + jobId);
                System.out.println("ERROR - > must not be able to write on slave. check if you database is start with --read-only");
                Assert.fail();
            } catch (SQLException e) {
                //normal exception
                connection.setReadOnly(false);
                stmt.execute("drop table if exists auroraDelete" + jobId);
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void testReplication() throws SQLException, InterruptedException {
        Connection connection = null;
        try {
            connection = getNewConnection(false);
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists auroraReadSlave" + jobId);
            stmt.execute("create table auroraReadSlave" + jobId + " (id int not null primary key auto_increment, test VARCHAR(10))");

            //wait to be sure slave have replicate data
            Thread.sleep(200);

            connection.setReadOnly(true);
            ResultSet rs = stmt.executeQuery("Select count(*) from auroraReadSlave" + jobId);
            Assert.assertTrue(rs.next());
            connection.setReadOnly(false);
            stmt.execute("drop table  if exists auroraReadSlave" + jobId);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }




    @Test
    public void testFailMaster() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=3&connectTimeout=1000", true);
            Statement stmt = connection.createStatement();
            int masterServerId = getServerId(connection);
            stopProxy(masterServerId);
            long stopTime = System.nanoTime();
            try {
                stmt.execute("SELECT 1");
                Assert.fail();
            } catch (SQLException e) {
                //normal error
            }
            Assert.assertTrue(!connection.isReadOnly());
            long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stopTime);
            Assert.assertTrue(duration < 20 * 1000);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Conj-79.
     *
     * @throws SQLException exception
     */
    @Test
    public void socketTimeoutTest() throws SQLException {
        Connection connection = null;
        try {
            // set a short connection timeout
            connection = getNewConnection("&socketTimeout=4000", false);

            PreparedStatement ps = connection.prepareStatement("SELECT 1");
            ResultSet rs = ps.executeQuery();
            rs.next();

            // wait for the connection to time out
            ps = connection.prepareStatement("DO sleep(5)");

            // a timeout should occur here
            try {
                rs = ps.executeQuery();
                Assert.fail();
            } catch (SQLException e) {
                // check that it's a timeout that occurs
                Assert.assertTrue(e.getMessage().contains("timed out"));
            }
            try {
                ps = connection.prepareStatement("SELECT 2");
                ps.execute();
            } catch (Exception e) {
                Assert.fail();
            }

            try {
                rs = ps.executeQuery();
            } catch (SQLException e) {
                Assert.fail();
            }

            // the connection should not be closed
            assertTrue(!connection.isClosed());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Conj-166
     * Connection error code must be thrown.
     *
     * @throws SQLException exception
     */
    @Test
    public void testAccessDeniedErrorCode() throws SQLException {
        try {
            DriverManager.getConnection(defaultUrl + "&retriesAllDown=6", "foouser", "foopwd");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertTrue("28000".equals(e.getSQLState()));
            Assert.assertTrue(1045 == e.getErrorCode());
        }
    }

    @Test
    public void testClearBlacklist() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection(true);
            connection.setReadOnly(true);
            int current = getServerId(connection);
            stopProxy(current);
            Statement st = connection.createStatement();
            try {
                st.execute("SELECT 1 ");
                //switch connection to master -> slave blacklisted
            } catch (SQLException e) {
                fail("must not have been here");
            }

            Protocol protocol = getProtocolFromConnection(connection);
            Assert.assertTrue(protocol.getProxy().getListener().getBlacklistKeys().size() == 1);
            assureBlackList();
            Assert.assertTrue(protocol.getProxy().getListener().getBlacklistKeys().size() == 0);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }


    @Test
    public void testCloseFail() throws Throwable {
        Connection connection = getNewConnection(true);
        connection.setReadOnly(true);
        int current = getServerId(connection);
        Protocol protocol = getProtocolFromConnection(connection);
        Assert.assertTrue(protocol.getProxy().getListener().getBlacklistKeys().size() == 0);
        stopProxy(current);
        connection.close();
        //check that after error connection have not been put to blacklist
        Assert.assertTrue(protocol.getProxy().getListener().getBlacklistKeys().size() == 0);
    }

}
