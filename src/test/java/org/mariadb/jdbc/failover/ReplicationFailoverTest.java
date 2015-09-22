package org.mariadb.jdbc.failover;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mariadb.jdbc.internal.common.UrlHAMode;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ReplicationFailoverTest extends BaseMultiHostTest {
    private Connection connection;
    private long testBeginTime;

    @Before
    public void init() throws SQLException {
        initialUrl = initialReplicationUrl;
        proxyUrl = proxyReplicationUrl;
        Assume.assumeTrue(initialReplicationUrl != null);
        connection = null;
        currentType = UrlHAMode.REPLICATION;
        testBeginTime = System.currentTimeMillis();
    }

    @After
    public void after() throws SQLException {
        assureProxy();
        assureBlackList(connection);
        if (connection != null) connection.close();

        log.debug("test time : " + (System.currentTimeMillis() - testBeginTime) + "ms");
    }

    @Test
    public void testWriteOnMaster() throws SQLException {
        connection = getNewConnection(false);
        Statement stmt = connection.createStatement();
        stmt.execute("drop table  if exists multinode");
        stmt.execute("create table multinode (id int not null primary key auto_increment, test VARCHAR(10))");
    }

    @Test
    public void testErrorWriteOnSlave() throws SQLException {
        connection = getNewConnection("&assureReadOnly=true",false);
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
        }
    }

    @Test
    public void randomConnection() throws Throwable {
        Map<String, MutableInt> connectionMap = new HashMap<String, MutableInt>();
        int masterId = -1;
        for (int i = 0; i < 20; i++) {
            connection = getNewConnection("&retriesAllDown=1", false);
            int serverId = getServerId(connection);
            log.debug("master server found " + serverId);
            if (i > 0) assertTrue(masterId == serverId);
            masterId = serverId;
            connection.setReadOnly(true);
            int replicaId = getServerId(connection);
            log.debug("++++++++++++slave  server found " + replicaId);
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
            log.debug(" ++++ Server " + key + " : " + connectionCount + " connections ");
            assertTrue(connectionCount > 1);
        }
    }

    @Test
    public void failoverSlaveToMaster() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1", true);
        int masterServerId = getServerId(connection);
        connection.setReadOnly(true);
        int slaveServerId = getServerId(connection);
        assertFalse(masterServerId == slaveServerId);
        stopProxy(slaveServerId);
        connection.createStatement().execute("SELECT 1");
        int currentServerId = getServerId(connection);

        log.debug("masterServerId = " + masterServerId + "/currentServerId = " + currentServerId);
        assertTrue(masterServerId == currentServerId);

        assertFalse(connection.isReadOnly());
    }

    @Test
    public void failoverSlaveToMasterFail() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&failOnReadOnly=true", true);
        int masterServerId = getServerId(connection);
        connection.setReadOnly(true);
        int slaveServerId = getServerId(connection);
        assertTrue(slaveServerId != masterServerId);
        connection.setReadOnly(false);
        stopProxy(masterServerId);

        long failTime = System.currentTimeMillis();
        connection.createStatement().execute("SELECT 1");
        assertTrue(System.currentTimeMillis() - failTime < 100);
        assertTrue(slaveServerId == getServerId(connection));
    }

    @Test
    public void pingReconnectAfterFailover() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&secondsBeforeRetryMaster=5&failOnReadOnly=false&queriesBeforeRetryMaster=50000", true);
        Statement st = connection.createStatement();
        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);

        try {
            st.execute("SELECT 1");
        } catch (SQLException e) {
        }

        connection.setReadOnly(true);
        st = connection.createStatement();
        restartProxy(masterServerId);
        try {
            connection.setReadOnly(false);
            fail();
        } catch (SQLException e) {
        }

        long stoppedTime = System.currentTimeMillis();

        boolean loop = true;
        while (loop) {
            try {
                Thread.sleep(250);
                log.debug("time : " + (System.currentTimeMillis() - stoppedTime) + "ms");
                int currentHost = getServerId(connection);
                if (masterServerId == currentHost) {
                    log.debug("reconnection with failover loop after : " + (System.currentTimeMillis() - stoppedTime) + "ms");
                    assertTrue((System.currentTimeMillis() - stoppedTime) > 5 * 1000);
                    loop = false;
                }
            } catch (SQLException e) {
            }
            if (System.currentTimeMillis() - stoppedTime > 20 * 1000) fail();
        }
    }

    @Test
    public void failoverDuringMasterSetReadOnly() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1", true);
        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);
        connection.setReadOnly(true);
        int slaveServerId = getServerId(connection);
        assertFalse(slaveServerId == masterServerId);
        assertTrue(connection.isReadOnly());
    }

    @Test
    public void failoverDuringSlaveSetReadOnly() throws Throwable {
        connection = getNewConnection(true);
        connection.setReadOnly(true);
        int slaveServerId = getServerId(connection);
        stopProxy(slaveServerId, 2000);
        connection.setReadOnly(false);
        int masterServerId = getServerId(connection);
        assertFalse(slaveServerId == masterServerId);
        assertFalse(connection.isReadOnly());
    }

    @Test()
    public void changeSlave() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1", true);
        int masterServerId = getServerId(connection);
        log.debug("master server_id = " + masterServerId);
        connection.setReadOnly(true);
        int firstSlaveId = getServerId(connection);
        log.debug("slave1 server_id = " + firstSlaveId);

        stopProxy(masterServerId);
        stopProxy(firstSlaveId);

        try {
            connection.createStatement().executeQuery("SELECT CONNECTION_ID()");
        } catch (SQLException e) {
            fail();
        }
    }

    @Test()
    public void masterWithoutFailover() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1", true);
        int masterServerId = getServerId(connection);
        log.debug("master server_id = " + masterServerId);
        connection.setReadOnly(true);
        int firstSlaveId = getServerId(connection);
        log.debug("slave1 server_id = " + firstSlaveId);
        connection.setReadOnly(false);

        stopProxy(masterServerId);
        stopProxy(firstSlaveId);

        try {
            connection.createStatement().executeQuery("SELECT CONNECTION_ID()");
            fail();
        } catch (SQLException e) {
            assertTrue(true);
        }
    }

    @Test
    public void failoverSlaveAndMasterWithAutoConnect() throws Throwable {
        connection = getNewConnection("&autoReconnect=true&retriesAllDown=1", true);

        //search actual server_id for master and slave
        int masterServerId = getServerId(connection);
        log.debug("master server_id = " + masterServerId);

        connection.setReadOnly(true);

        int firstSlaveId = getServerId(connection);
        log.debug("slave1 server_id = " + firstSlaveId);

        stopProxy(masterServerId);
        stopProxy(firstSlaveId);

        //must reconnect to the second slave without error
        connection.createStatement().execute("SELECT 1");
        int currentSlaveId = getServerId(connection);
        log.debug("currentSlaveId server_id = " + currentSlaveId);
        assertTrue(currentSlaveId != firstSlaveId);
        assertTrue(currentSlaveId != masterServerId);
    }

    @Test
    public void failoverMasterWithAutoConnect() throws Throwable {
        connection = getNewConnection("&autoReconnect=true&retriesAllDown=1", true);
        int masterServerId = getServerId(connection);

        stopProxy(masterServerId, 250);
        //with autoreconnect, the connection must reconnect automatically
        int currentServerId = getServerId(connection);

        assertTrue(currentServerId == masterServerId);
        assertFalse(connection.isReadOnly());
    }

    @Test
    public void checkReconnectionToMasterAfterQueryNumber() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&secondsBeforeRetryMaster=3000&queriesBeforeRetryMaster=10&failOnReadOnly=true", true);
        Statement st = connection.createStatement();
        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);
        try {
            st.execute("SELECT 1");
        } catch (SQLException e) {
            fail();
        }
        assertTrue(connection.isReadOnly());

        restartProxy(masterServerId);

        //not in autoreconnect mode, so must wait for query more than queriesBeforeRetryMaster
        for (int i = 1; i < 10; i++) {
            try {
                st.execute("SELECT 1");
                log.debug("i=" + i);
                assertTrue(connection.isReadOnly());
            } catch (SQLException e) {
                fail();
            }
        }
        Thread.sleep(5000);
        long startTime = System.currentTimeMillis();
        connection.setReadOnly(false);
        log.debug(" time = " + (System.currentTimeMillis() - startTime));
        assertTrue(System.currentTimeMillis() - startTime < 4000);

    }

    @Test
    public void reconnectMasterAfterFailover() throws Throwable {
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
        }
    }

    @Test
    public void writeToSlaveAfterFailover() throws Throwable {
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
        }
    }


    @Test
    public void checkBackOnMasterOnSlaveFail() throws Throwable {
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
                    log.debug("reconnection to master with failover loop after : " + (System.currentTimeMillis() - stoppedTime) + "ms");
                    assertTrue((System.currentTimeMillis() - stoppedTime) > 10 * 1000);
                    loop = false;
                }
            } catch (SQLException e) {
            }
            if (System.currentTimeMillis() - stoppedTime > 30 * 1000) fail();
        }
    }


    @Test()
    public void checkNoSwitchConnectionDuringTransaction() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&autoReconnect=true", false);
        Statement st = connection.createStatement();

        st.execute("drop table  if exists multinodeTransaction2");
        st.execute("create table multinodeTransaction2 (id int not null primary key , amount int not null) ENGINE = InnoDB");
        connection.setAutoCommit(false);
        st.execute("insert into multinodeTransaction2 (id, amount) VALUE (1 , 100)");

        try {
            //in transaction, so must trow an error
            connection.setReadOnly(true);
            fail();
        } catch (SQLException e) {
        }
    }

    @Test
    public void failoverMasterWithAutoConnectAndTransaction() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&autoReconnect=true", true);
        Statement st = connection.createStatement();

        int masterServerId = getServerId(connection);
        st.execute("drop table  if exists multinodeTransaction");
        st.execute("create table multinodeTransaction (id int not null primary key , amount int not null) ENGINE = InnoDB");
        connection.setAutoCommit(false);
        st.execute("insert into multinodeTransaction (id, amount) VALUE (1 , 100)");
        stopProxy(masterServerId);
        assertTrue(inTransaction(connection));
        try {
            //with autoreconnect but in transaction, query must throw an error
            st.execute("insert into multinodeTransaction (id, amount) VALUE (2 , 10)");
            fail();
        } catch (SQLException e) {
        }
        restartProxy(masterServerId);
        try {
            st = connection.createStatement();
            // will try a ping, if ok, if not, transaction is considered be lost
            st.execute("insert into multinodeTransaction (id, amount) VALUE (2 , 10)");
        } catch (SQLException e) {
            fail();
        }
    }

    @Test
    public void testFailNotOnSlave() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&autoReconnectMaster=true&failOnReadOnly=false", true);
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
    }

    @Test
    public void testAutoReconnectMasterFailSlave() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&failOnReadOnly=true", true);
        Statement stmt = connection.createStatement();
        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);
        try {
            stmt.execute("SELECT 1");
        } catch (SQLException e) {
            fail();
        }
        assertTrue(connection.isReadOnly());
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
