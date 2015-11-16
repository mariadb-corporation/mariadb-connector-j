package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ReplicationFailoverTest extends BaseMultiHostTest {

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
    public void testWriteOnMasterReplication() throws SQLException {
        Connection connection = null;
        try {
            connection = getNewConnection(false);
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists replicationmultinode");
            stmt.execute("create table replicationmultinode (id int not null primary key auto_increment, test VARCHAR(10))");
            stmt.execute("drop table  if exists replicationmultinode");
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void testErrorWriteOnSlave() throws SQLException {
        Connection connection = null;
        try {
            connection = getNewConnection("&assureReadOnly=true", false);
            connection.setReadOnly(true);
            Statement stmt = connection.createStatement();
            assertTrue(connection.isReadOnly());
            try {
                if (!isMariadbServer(connection) || !requireMinimumVersion(connection, 10, 0)) {
                    //on version > 10 use SESSION READ-ONLY, before no control
                    Assume.assumeTrue(false);
                }
                stmt.execute("drop table  if exists multinode4");
                log.error("ERROR - > must not be able to write on slave ");
                fail();
            } catch (SQLException e) {
                //normal exception
            }
        } finally {
            connection.close();
        }
    }

    @Test
    public void randomConnection() throws Throwable {
        Map<String, MutableInt> connectionMap = new HashMap<String, MutableInt>();
        int masterId = -1;
        for (int i = 0; i < 20; i++) {
            Connection connection = connection = getNewConnection("&retriesAllDown=1", false);
            int serverId = getServerId(connection);
            log.trace("master server found " + serverId);
            if (i > 0) {
                assertTrue(masterId == serverId);
            }
            masterId = serverId;
            connection.setReadOnly(true);
            int replicaId = getServerId(connection);
            log.trace("++++++++++++slave  server found " + replicaId);
            MutableInt count = connectionMap.get(String.valueOf(replicaId));
            if (count == null) {
                connectionMap.put(String.valueOf(replicaId), new MutableInt());
            } else {
                count.increment();
            }
            connection.close();
        }

        assertTrue(connectionMap.size() >= 2);
        for (String key : connectionMap.keySet()) {
            Integer connectionCount = connectionMap.get(key).get();
            log.trace(" ++++ Server " + key + " : " + connectionCount + " connections ");
            assertTrue(connectionCount > 1);
        }
    }

    @Test
    public void failoverSlaveToMaster() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1", true);
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            int slaveServerId = getServerId(connection);
            assertFalse(masterServerId == slaveServerId);
            stopProxy(slaveServerId);
            connection.createStatement().execute("SELECT 1");
            int currentServerId = getServerId(connection);

            log.trace("masterServerId = " + masterServerId + "/currentServerId = " + currentServerId);
            assertTrue(masterServerId == currentServerId);

            assertFalse(connection.isReadOnly());
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
            connection = getNewConnection("&retriesAllDown=1&secondsBeforeRetryMaster=5&queriesBeforeRetryMaster=50000", true);
            Statement st = connection.createStatement();
            final int masterServerId = getServerId(connection);
            stopProxy(masterServerId);
            long stoppedTime = System.currentTimeMillis();

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
                fail();
            } catch (SQLException e) {
                //normal exception
            }


            boolean loop = true;
            while (loop) {
                try {
                    Thread.sleep(250);
                    log.trace("time : " + (System.currentTimeMillis() - stoppedTime) + "ms");
                    int currentHost = getServerId(connection);
                    if (masterServerId == currentHost) {
                        log.trace("reconnection with failover loop after : " + (System.currentTimeMillis() - stoppedTime)
                                + "ms");
                        assertTrue((System.currentTimeMillis() - stoppedTime) > 5 * 1000);
                        loop = false;
                    }
                } catch (SQLException e) {
                    //eat exception
                }
                if (System.currentTimeMillis() - stoppedTime > 20 * 1000) {
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
    public void failoverDuringMasterSetReadOnly() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1", true);
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

    @Test
    public void failoverDuringSlaveSetReadOnly() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection(true);
            connection.setReadOnly(true);
            int slaveServerId = getServerId(connection);
            stopProxy(slaveServerId, 2000);
            connection.setReadOnly(false);
            int masterServerId = getServerId(connection);
            assertFalse(slaveServerId == masterServerId);
            assertFalse(connection.isReadOnly());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test()
    public void changeSlave() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1", true);
            int masterServerId = getServerId(connection);
            log.trace("master server_id = " + masterServerId);
            connection.setReadOnly(true);
            int firstSlaveId = getServerId(connection);
            log.trace("slave1 server_id = " + firstSlaveId);

            stopProxy(masterServerId);
            stopProxy(firstSlaveId);

            try {
                connection.createStatement().executeQuery("SELECT CONNECTION_ID()");
            } catch (SQLException e) {
                fail();
            }
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
            connection = getNewConnection("&retriesAllDown=1", true);
            int masterServerId = getServerId(connection);
            log.trace("master server_id = " + masterServerId);
            connection.setReadOnly(true);
            int firstSlaveId = getServerId(connection);
            log.trace("slave1 server_id = " + firstSlaveId);
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
    public void failoverSlaveAndMasterWithAutoConnect() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&autoReconnect=true&retriesAllDown=1", true);

            //search actual server_id for master and slave
            int masterServerId = getServerId(connection);
            log.trace("master server_id = " + masterServerId);

            connection.setReadOnly(true);

            int firstSlaveId = getServerId(connection);
            log.trace("slave1 server_id = " + firstSlaveId);

            stopProxy(masterServerId);
            stopProxy(firstSlaveId);

            //must reconnect to the second slave without error
            connection.createStatement().execute("SELECT 1");
            int currentSlaveId = getServerId(connection);
            log.trace("currentSlaveId server_id = " + currentSlaveId);
            assertTrue(currentSlaveId != firstSlaveId);
            assertTrue(currentSlaveId != masterServerId);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void failoverMasterWithAutoConnect() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&autoReconnect=true&retriesAllDown=1", true);
            int masterServerId = getServerId(connection);

            stopProxy(masterServerId, 250);
            //with autoreconnect, the connection must reconnect automatically
            int currentServerId = getServerId(connection);

            assertTrue(currentServerId == masterServerId);
            assertFalse(connection.isReadOnly());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void reconnectMasterAfterFailover() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1", true);
            //if super user can write on slave
            Assume.assumeTrue(!hasSuperPrivilege(connection, "reconnectMasterAfterFailover"));
            Statement st = connection.createStatement();
            st.execute("drop table  if exists multinode2");
            st.execute("create table multinode2 (id int not null primary key , amount int not null) ENGINE = InnoDB");
            st.execute("insert into multinode2 (id, amount) VALUE (1 , 100)");

            int masterServerId = getServerId(connection);
            long stopTime = System.currentTimeMillis();
            stopProxy(masterServerId, 10000);
            try {
                st.execute("insert into multinode2 (id, amount) VALUE (2 , 100)");
                assertTrue(System.currentTimeMillis() - stopTime > 10);
                assertTrue(System.currentTimeMillis() - stopTime < 20);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void writeToSlaveAfterFailover() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1", true);
            //if super user can write on slave
            Assume.assumeTrue(!hasSuperPrivilege(connection, "writeToSlaveAfterFailover"));
            Statement st = connection.createStatement();
            st.execute("drop table  if exists multinode2");
            st.execute("create table multinode2 (id int not null primary key , amount int not null) ENGINE = InnoDB");
            st.execute("insert into multinode2 (id, amount) VALUE (1 , 100)");

            int masterServerId = getServerId(connection);

            stopProxy(masterServerId);
            try {
                st.execute("insert into multinode2 (id, amount) VALUE (2 , 100)");
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
    public void checkBackOnMasterOnSlaveFail() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1&secondsBeforeRetryMaster=10&failOnReadOnly=true", true);
            Statement st = connection.createStatement();
            int masterServerId = getServerId(connection);
            stopProxy(masterServerId);

            try {
                st.execute("SELECT 1");
                assertTrue(connection.isReadOnly());
            } catch (SQLException e) {
                fail();
            }

            long stoppedTime = System.currentTimeMillis();
            restartProxy(masterServerId);
            boolean loop = true;
            while (loop) {
                Thread.sleep(250);
                try {
                    if (!connection.isReadOnly()) {
                        log.trace("reconnection to master with failover loop after : " + (System.currentTimeMillis()
                                - stoppedTime) + "ms");
                        assertTrue((System.currentTimeMillis() - stoppedTime) > 10 * 1000);
                        loop = false;
                    }
                } catch (SQLException e) {
                    //eat exception
                }
                if (System.currentTimeMillis() - stoppedTime > 30 * 1000) {
                    fail();
                }
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }


    @Test()
    public void checkNoSwitchConnectionDuringTransaction() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1&autoReconnect=true", false);
            Statement st = connection.createStatement();

            st.execute("drop table  if exists multinodeTransaction2");
            st.execute("create table multinodeTransaction2 (id int not null primary key , amount int not null) "
                    + "ENGINE = InnoDB");
            connection.setAutoCommit(false);
            st.execute("insert into multinodeTransaction2 (id, amount) VALUE (1 , 100)");

            try {
                //in transaction, so must trow an error
                connection.setReadOnly(true);
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
    public void failoverMasterWithAutoConnectAndTransaction() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1&autoReconnect=true", true);
            Statement st = connection.createStatement();

            final int masterServerId = getServerId(connection);
            st.execute("drop table  if exists multinodeTransaction");
            st.execute("create table multinodeTransaction (id int not null primary key , amount int not null) "
                    + "ENGINE = InnoDB");
            connection.setAutoCommit(false);
            st.execute("insert into multinodeTransaction (id, amount) VALUE (1 , 100)");
            stopProxy(masterServerId);
            assertTrue(inTransaction(connection));
            try {
                //with autoreconnect but in transaction, query must throw an error
                st.execute("insert into multinodeTransaction (id, amount) VALUE (2 , 10)");
                fail();
            } catch (SQLException e) {
                //normal exception
            }
            restartProxy(masterServerId);
            try {
                st = connection.createStatement();
                // will try a ping, if ok, if not, transaction is considered be lost
                st.execute("insert into multinodeTransaction (id, amount) VALUE (2 , 10)");
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
    public void testFailNotOnSlave() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1&autoReconnectMaster=true", true);
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

    class MutableInt {
        int value = 1; // note that we start at 1 since we're counting

        public void increment() {
            ++value;
        }

        public int get() {
            return value;
        }
    }

}
