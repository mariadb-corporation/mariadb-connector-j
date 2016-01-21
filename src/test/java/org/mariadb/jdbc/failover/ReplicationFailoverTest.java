package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ReplicationFailoverTest extends BaseReplication {

    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void beforeClass2() throws SQLException {
        proxyUrl = proxyReplicationUrl;
        Assume.assumeTrue(initialReplicationUrl != null);
    }

    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @Before
    public void init() throws SQLException {
        defaultUrl = initialReplicationUrl;
        currentType = HaMode.REPLICATION;
    }

    @Test
    public void readOnlyPropagatesToServerAlias() throws SQLException {
        assureReadOnly(true);
    }

    @Test
    public void assureReadOnly() throws SQLException {
        assureReadOnly(false);
    }

    /**
     * Test assureReadOnly / readOnlyPropagatesToServer alias.
     *
     * @param useAlias use alias readOnlyPropagatesToServer ?
     * @throws SQLException if any exception
     */
    public void assureReadOnly(boolean useAlias) throws SQLException {
        Connection connection = null;
        try {
            connection = getNewConnection(useAlias ? "&readOnlyPropagatesToServer=true" : "&assureReadOnly=true", false);
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists replicationDelete" + jobId);
            stmt.execute("create table replicationDelete" + jobId + " (id int not null primary key auto_increment, test VARCHAR(10))");
            connection.setReadOnly(true);
            assertTrue(connection.isReadOnly());
            try {
                if (!isMariaDbServer(connection) || !requireMinimumVersion(connection, 5, 7)) {
                    //on version >= 5.7 use SESSION READ-ONLY, before no control
                    Assume.assumeTrue(false);
                }
                connection.createStatement().execute("drop table  if exists replicationDelete" + jobId);
                fail();
            } catch (SQLException e) {
                //normal exception
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }


    @Test
    public void pingReconnectAfterFailover() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true);
            Statement st = connection.createStatement();
            final int masterServerId = getServerId(connection);
            stopProxy(masterServerId);

            try {
                st.execute("SELECT 1");
            } catch (SQLException e) {
                //normal exception
            }

            connection.setReadOnly(true);
            st = connection.createStatement();
            restartProxy(masterServerId);
            try {
                connection.setReadOnly(false);
            } catch (SQLException e) {
                fail();
            }

        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void failoverDuringMasterSetReadOnly() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true);
            int masterServerId = getServerId(connection);
            stopProxy(masterServerId);
            connection.setReadOnly(true);
            int slaveServerId = getServerId(connection);
            assertFalse(slaveServerId == masterServerId);
            assertTrue(connection.isReadOnly());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test()
    public void masterWithoutFailover() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true);
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            int firstSlaveId = getServerId(connection);
            connection.setReadOnly(false);

            stopProxy(masterServerId);
            stopProxy(firstSlaveId);

            try {
                connection.createStatement().executeQuery("SELECT CONNECTION_ID()");
                fail();
            } catch (SQLException e) {
                assertTrue(true);
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void checkBackOnMasterOnSlaveFail() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6&failOnReadOnly=true&connectTimeout=1000&socketTimeout=1000", true);
            Statement st = connection.createStatement();
            int masterServerId = getServerId(connection);
            stopProxy(masterServerId);

            try {
                st.execute("SELECT 1");
                assertTrue(connection.isReadOnly());
            } catch (SQLException e) {
                fail();
            }

            long stoppedTime = System.nanoTime();
            restartProxy(masterServerId);
            boolean loop = true;
            while (loop) {
                Thread.sleep(250);
                try {
                    connection.setReadOnly(true);
                    loop = false;
                } catch (SQLException e) {
                    //eat exception
                }
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stoppedTime);
                if (duration > 15 * 1000) {
                    fail();
                }
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }


    @Test
    public void testFailNotOnSlave() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true);
            Statement stmt = connection.createStatement();
            int masterServerId = getServerId(connection);
            stopProxy(masterServerId);
            try {
                stmt.execute("SELECT 1");
                fail();
            } catch (SQLException e) {
                //normal error
            }
            assertTrue(!connection.isReadOnly());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

}
