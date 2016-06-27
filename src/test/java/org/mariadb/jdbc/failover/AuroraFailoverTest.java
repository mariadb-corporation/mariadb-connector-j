package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.MariaDbServerPreparedStatement;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.sql.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

/**
 * Aurora test suite.
 * Some environment parameter must be set :
 * - defaultAuroraUrl : example -DdefaultAuroraUrl=jdbc:mysql:aurora://instance-1.xxxx,instance-2.xxxx/testj?user=userName&password=userPwd
 * - AURORA_ACCESS_KEY = access key
 * - AURORA_SECRET_KEY = secret key
 * - AURORA_CLUSTER_IDENTIFIER = cluster identifier. example : -DAURORA_CLUSTER_IDENTIFIER=instance-1-cluster
 *
 * "AURORA" environment variable must be set to a value
 */
public class AuroraFailoverTest extends BaseReplication {

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
            Assert.assertEquals(1045, e.getErrorCode());
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

    /**
     * Test failover on prepareStatement on slave.
     * PrepareStatement must fall back on master, and back on slave when a new slave connection is up again.
     * @throws Throwable if any error occur
     */
    @Test
    public void failoverPrepareStatementOnSlave() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&validConnectionTimeout=120"
                    + "&socketTimeout=1000"
                    + "&failoverLoopRetries=120"
                    + "&connectTimeout=250"
                    + "&loadBalanceBlacklistTimeout=50", false);

            connection.setReadOnly(true);

            //prepareStatement on slave connection
            PreparedStatement preparedStatement = connection.prepareStatement("select @@innodb_read_only as is_read_only");

            launchAuroraFailover();

            //test failover
            int nbExecutionOnSlave = 0;
            int nbExecutionOnMasterFirstFailover = 0;
            int nbExecutionOnMasterSecondFailover = 0;

            //Goal is to check that on a failover, master connection will be used, and slave will be used back when up.
            //check on 2 failover
            while (nbExecutionOnSlave + nbExecutionOnMasterFirstFailover < 5000) {
                ResultSet rs = preparedStatement.executeQuery();
                rs.next();
                if (rs.getInt(1) == 1) {
                    nbExecutionOnSlave++;
                    if (nbExecutionOnMasterFirstFailover > 0) {
                        break;
                    }
                } else {
                    nbExecutionOnMasterFirstFailover++;
                }
            }
            launchAuroraFailover();
            while (nbExecutionOnSlave + nbExecutionOnMasterSecondFailover < 5000) {
                ResultSet rs = preparedStatement.executeQuery();
                rs.next();
                if (rs.getInt(1) == 1) {
                    nbExecutionOnSlave++;
                    if (nbExecutionOnMasterSecondFailover > 0) {
                        break;
                    }
                } else {
                    nbExecutionOnMasterSecondFailover++;
                }
            }
            System.out.println("back using slave connection. nbExecutionOnSlave=" + nbExecutionOnSlave
                    + " nbExecutionOnMasterFirstFailover=" + nbExecutionOnMasterFirstFailover
                    + " nbExecutionOnMasterSecondFailover=" + nbExecutionOnMasterSecondFailover);
            assertTrue(nbExecutionOnSlave + nbExecutionOnMasterFirstFailover + nbExecutionOnMasterSecondFailover < 2000);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }


    /**
     * Test that master complete failover (not just a network error) server will changed, PrepareStatement will be closed
     * and that PrepareStatement cache is invalidated.
     *
     * @throws Throwable if any error occur
     */
    @Test
    public void failoverPrepareStatementOnMasterWithException() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&validConnectionTimeout=120"
                    + "&socketTimeout=1000"
                    + "&failoverLoopRetries=120"
                    + "&connectTimeout=250"
                    + "&loadBalanceBlacklistTimeout=50", false);

            int nbExceptionBeforeUp = 0;
            boolean failLaunched = false;
            PreparedStatement preparedStatement1 = connection.prepareStatement("select ?");
            assertEquals(1, getPrepareResult((MariaDbServerPreparedStatement) preparedStatement1).getStatementId());
            PreparedStatement otherPrepareStatement = connection.prepareStatement(" select 1");

            while (nbExceptionBeforeUp < 1000) {
                try {
                    PreparedStatement preparedStatement = connection.prepareStatement(" select 1");
                    preparedStatement.executeQuery();
                    int currentPrepareId = getPrepareResult((MariaDbServerPreparedStatement) preparedStatement).getStatementId();
                    if (nbExceptionBeforeUp > 0) {
                        assertEquals(1, currentPrepareId);
                        break;
                    }
                    if (!failLaunched) {
                        launchAuroraFailover();
                        failLaunched = true;
                    }
                    assertEquals(2, currentPrepareId);

                } catch (SQLException e) {
                    nbExceptionBeforeUp++;
                }
            }
            assertTrue(nbExceptionBeforeUp < 50);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Same than failoverPrepareStatementOnMasterWithException, but since query is a select, mustn't throw an exception.
     *
     * @throws Throwable if any error occur
     */
    @Test
    public void failoverPrepareStatementOnMaster() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&validConnectionTimeout=120"
                    + "&socketTimeout=1000"
                    + "&failoverLoopRetries=120"
                    + "&connectTimeout=250"
                    + "&loadBalanceBlacklistTimeout=50", false);

            int nbExecutionBeforeRePrepared = 0;
            boolean failLaunched = false;
            PreparedStatement preparedStatement1 = connection.prepareStatement("select ?");
            assertEquals(1, getPrepareResult((MariaDbServerPreparedStatement) preparedStatement1).getStatementId());
            PreparedStatement otherPrepareStatement = connection.prepareStatement("select @@innodb_read_only as is_read_only");
            int currentPrepareId = 0;
            while (nbExecutionBeforeRePrepared < 1000) {
                PreparedStatement preparedStatement = connection.prepareStatement("select @@innodb_read_only as is_read_only");
                preparedStatement.executeQuery();
                currentPrepareId = getPrepareResult((MariaDbServerPreparedStatement) preparedStatement).getStatementId();

                if (nbExecutionBeforeRePrepared == 0) {
                    assertEquals(2, currentPrepareId);
                } else {
                    if (!failLaunched) {
                        launchAuroraFailover();
                        failLaunched = true;
                    }
                    if (currentPrepareId == 1) break;
                }
                nbExecutionBeforeRePrepared++;
            }
            assertEquals(1, currentPrepareId);
            assertTrue(nbExecutionBeforeRePrepared < 200);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
