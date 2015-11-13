package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class AuroraFailoverTest extends BaseMultiHostTest {
    private Connection connection;

    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @Before
    public void init() throws SQLException {
        initialUrl = initialAuroraUrl;
        proxyUrl = proxyAuroraUrl;
        currentType = HaMode.AURORA;
        Assume.assumeTrue(initialAuroraUrl != null);
        connection = null;
    }

    /**
     * After.
     * @throws SQLException exception
     */
    @After
    public void after() throws SQLException {
        assureProxy();
        if (connection != null) {
            connection.close();
            assureBlackList(connection);
        }
    }

    @Test
    public void testWriteOnMaster() throws SQLException {
        connection = getNewConnection(false);
        Statement stmt = connection.createStatement();
        stmt.execute("drop table  if exists multinode");
        stmt.execute("create table multinode (id int not null primary key auto_increment, test VARCHAR(10))");
    }

    @Test
    public void testErrorWriteOnReplica() throws SQLException {
        connection = getNewConnection(false);
        connection.setReadOnly(true);
        Statement stmt = connection.createStatement();
        Assert.assertTrue(connection.isReadOnly());
        try {
            stmt.execute("drop table  if exists multinode4");
            log.error("ERROR - > must not be able to write on slave. check if you database is start with --read-only");
            Assert.fail();
        } catch (SQLException e) {
            //normal exception
        }
    }

    @Test
    public void testReplication() throws SQLException, InterruptedException {
        connection = getNewConnection(false);
        Statement stmt = connection.createStatement();
        stmt.execute("drop table  if exists multinodeReadSlave");
        stmt.execute("create table multinodeReadSlave (id int not null primary key auto_increment, test VARCHAR(10))");

        //wait to be sure slave have replicate data
        Thread.sleep(200);

        connection.setReadOnly(true);

        ResultSet rs = stmt.executeQuery("Select count(*) from multinodeReadSlave");
        Assert.assertTrue(rs.next());
    }


    @Test
    public void randomConnection() throws Throwable {
        Map<String, MutableInt> connectionMap = new HashMap<String, MutableInt>();
        int masterId = -1;
        for (int i = 0; i < 20; i++) {
            connection = getNewConnection(false);
            int serverId = getServerId(connection);
            log.trace("master server found " + serverId);
            if (i > 0) {
                Assert.assertTrue(masterId == serverId);
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

        Assert.assertTrue(connectionMap.size() >= 2);
        for (String key : connectionMap.keySet()) {
            Integer connectionCount = connectionMap.get(key).get();
            log.trace(" ++++ Server " + key + " : " + connectionCount + " connections ");
            Assert.assertTrue(connectionCount > 1);
        }
        log.trace("randomConnection OK");
    }


    @Test
    public void failoverSlaveToMaster() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1", true);
        int masterServerId = getServerId(connection);
        connection.setReadOnly(true);
        int slaveServerId = getServerId(connection);
        Assert.assertFalse(masterServerId == slaveServerId);
        stopProxy(slaveServerId);
        connection.createStatement().execute("SELECT 1");
        int currentServerId = getServerId(connection);

        log.trace("masterServerId = " + masterServerId + "/currentServerId = " + currentServerId);
        Assert.assertTrue(masterServerId == currentServerId);

        Assert.assertFalse(connection.isReadOnly());
    }


    @Test
    public void failoverSlaveToMasterFail() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&secondsBeforeRetryMaster=1", true);
        int masterServerId = getServerId(connection);
        connection.setReadOnly(true);
        int slaveServerId = getServerId(connection);
        Assert.assertTrue(slaveServerId != masterServerId);

        connection.setCatalog("mysql"); //to be sure there will be a query, and so an error when switching connection
        stopProxy(masterServerId);
        try {
            //must not throw error until there is a query
            connection.setReadOnly(false);
            Assert.fail();
        } catch (SQLException e) {
            //normal exception
        }
    }

    @Test
    public void pingReconnectAfterRestart() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&secondsBeforeRetryMaster=1&queriesBeforeRetryMaster=50000", true);
        Statement st = connection.createStatement();
        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);

        long stoppedTime = System.currentTimeMillis();
        try {
            st.execute("SELECT 1");
        } catch (SQLException e) {
            //normal exception
        }
        restartProxy(masterServerId);
        long restartTime = System.currentTimeMillis();

        boolean loop = true;
        while (loop) {
            if (!connection.isClosed()) {
                log.trace("reconnection with failover loop after : "
                        + (System.currentTimeMillis() - stoppedTime) + "ms");
                loop = false;
            }
            if (System.currentTimeMillis() - restartTime > 15 * 1000) {
                Assert.fail();
            }
            Thread.sleep(250);
        }
    }


    @Test
    public void failoverDuringMasterSetReadOnly() throws Throwable {
        int masterServerId = -1;
        connection = getNewConnection("&retriesAllDown=1", true);
        masterServerId = getServerId(connection);

        stopProxy(masterServerId);

        connection.setReadOnly(true);

        int slaveServerId = getServerId(connection);

        Assert.assertFalse(slaveServerId == masterServerId);
        Assert.assertTrue(connection.isReadOnly());
    }

    @Test
    public void failoverDuringSlaveSetReadOnly() throws Throwable {
        connection = getNewConnection(true);
        connection.setReadOnly(true);
        int slaveServerId = getServerId(connection);

        stopProxy(slaveServerId, 2000);

        connection.setReadOnly(false);

        int masterServerId = getServerId(connection);

        Assert.assertFalse(slaveServerId == masterServerId);
        Assert.assertFalse(connection.isReadOnly());
    }

    @Test()
    public void failoverSlaveAndMasterWithoutAutoConnect() throws Throwable {
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
            Assert.fail();
        }
    }

    @Test
    public void reconnectSlaveAndMasterWithAutoConnect() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&autoReconnect=true", true);

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
        Assert.assertTrue(currentSlaveId != firstSlaveId);
        Assert.assertTrue(currentSlaveId != masterServerId);
    }


    @Test
    public void failoverMasterWithAutoConnect() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&autoReconnect=true", true);
        int masterServerId = getServerId(connection);

        stopProxy(masterServerId, 250);
        //with autoreconnect, the connection must reconnect automatically
        int currentServerId = getServerId(connection);

        Assert.assertTrue(currentServerId == masterServerId);
        Assert.assertFalse(connection.isReadOnly());
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
            Assert.assertTrue(System.currentTimeMillis() - stopTime > 10);
            Assert.assertTrue(System.currentTimeMillis() - stopTime < 20);
        } catch (SQLException e) {
            //eat exception
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
            Assert.fail();
        } catch (SQLException e) {
            //normal exception
        }
    }


    @Test()
    public void checkNoSwitchConnectionDuringTransaction() throws Throwable {
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
            Assert.fail();
        } catch (SQLException e) {
            //normal exception
        }
    }

    @Test
    public void failoverMasterWithAutoConnectAndTransaction() throws Throwable {
        connection = getNewConnection("&autoReconnect=true&retriesAllDown=1", true);
        Statement st = connection.createStatement();

        final int masterServerId = getServerId(connection);
        st.execute("drop table  if exists multinodeTransaction");
        st.execute("create table multinodeTransaction (id int not null primary key , amount int not null) "
                + "ENGINE = InnoDB");
        connection.setAutoCommit(false);
        st.execute("insert into multinodeTransaction (id, amount) VALUE (1 , 100)");
        stopProxy(masterServerId);
        Assert.assertTrue(inTransaction(connection));
        try {
            // will to execute the query. if there is a connection error, try a ping, if ok, good, query relaunched.
            // If not good, transaction is considered be lost
            st.execute("insert into multinodeTransaction (id, amount) VALUE (2 , 10)");
            Assert.fail();
        } catch (SQLException e) {
            log.trace("normal error : " + e.getMessage());
        }
        restartProxy(masterServerId);
        try {
            st = connection.createStatement();
            st.execute("insert into multinodeTransaction (id, amount) VALUE (2 , 10)");
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testFailMaster() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&autoReconnect=true", true);
        Statement stmt = connection.createStatement();
        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);
        long stopTime = System.currentTimeMillis();
        try {
            stmt.execute("SELECT 1");
            Assert.fail();
        } catch (SQLException e) {
            //normal error
        }
        Assert.assertTrue(!connection.isReadOnly());
        Assert.assertTrue(System.currentTimeMillis() - stopTime < 20 * 1000);
    }

    /**
     * Conj-79.
     *
     * @throws SQLException exception
     */
    @Test
    public void socketTimeoutTest() throws SQLException {
        // set a short connection timeout
        connection = getNewConnection("&socketTimeout=4000", false);

        PreparedStatement ps = connection.prepareStatement("SELECT 1");
        ResultSet rs = ps.executeQuery();
        rs.next();

        // wait for the connection to time out
        ps = connection.prepareStatement("SELECT sleep(5)");

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
            DriverManager.getConnection(initialUrl + "&retriesAllDown=1", "foouser", "foopwd");
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertTrue("28000".equals(e.getSQLState()));
            Assert.assertTrue(1045 == e.getErrorCode());
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
