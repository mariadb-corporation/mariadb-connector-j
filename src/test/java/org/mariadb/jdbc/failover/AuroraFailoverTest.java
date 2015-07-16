package org.mariadb.jdbc.failover;

import org.junit.*;
import org.junit.Test;
import org.mariadb.jdbc.internal.mysql.FailoverProxy;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class AuroraFailoverTest extends BaseMultiHostTest {
    private Connection connection;
    private long testBeginTime;

    @Before
    public void init() throws SQLException {
        initialUrl = initialAuroraUrl;
        proxyUrl = proxyAuroraUrl;
        currentType = TestType.AURORA;
        testBeginTime = System.currentTimeMillis();
        Assume.assumeTrue(initialAuroraUrl != null);
        connection = null;
    }

    @After
    public void after() throws SQLException {
        assureProxy();
        if (connection != null) {
            connection.close();
            assureBlackList(connection);
        }
        log.fine("test time : " + (System.currentTimeMillis() - testBeginTime) + "ms");
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
            log.severe("ERROR - > must not be able to write on slave --> check if you database is start with --read-only");
            Assert.fail();
        } catch (SQLException e) {
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
            log.fine("master server found " + serverId);
            if (i > 0) Assert.assertTrue(masterId == serverId);
            masterId = serverId;
            connection.setReadOnly(true);
            int replicaId = getServerId(connection);
            log.fine("++++++++++++slave  server found " + replicaId);
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
            log.fine(" ++++ Server " + key + " : " + connectionCount + " connections ");
            Assert.assertTrue(connectionCount > 1);
        }
        log.fine("randomConnection OK");
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

        log.fine("masterServerId = " + masterServerId + "/currentServerId = " + currentServerId);
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
        }
    }

    @Test
    public void pingReconnectAfterRestart() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&secondsBeforeRetryMaster=1&failOnReadOnly=false&queriesBeforeRetryMaster=50000", true);
        Statement st = connection.createStatement();
        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);

        long stoppedTime = System.currentTimeMillis();
        try {
            st.execute("SELECT 1");
        } catch (SQLException e) {
        }
        restartProxy(masterServerId);
        long restartTime = System.currentTimeMillis();

        boolean loop = true;
        while (loop) {
            if (!connection.isClosed()) {
                log.fine("reconnection with failover loop after : " + (System.currentTimeMillis() - stoppedTime) + "ms");
                loop = false;
            }
            if (System.currentTimeMillis() - restartTime > 15 * 1000) Assert.fail();
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
        log.fine("master server_id = " + masterServerId);
        connection.setReadOnly(true);
        int firstSlaveId = getServerId(connection);
        log.fine("slave1 server_id = " + firstSlaveId);

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
        log.fine("master server_id = " + masterServerId);

        connection.setReadOnly(true);

        int firstSlaveId = getServerId(connection);
        log.fine("slave1 server_id = " + firstSlaveId);

        stopProxy(masterServerId);
        stopProxy(firstSlaveId);

        //must reconnect to the second slave without error
        connection.createStatement().execute("SELECT 1");
        int currentSlaveId = getServerId(connection);
        log.fine("currentSlaveId server_id = " + currentSlaveId);
        Assert.assertTrue(currentSlaveId != firstSlaveId);
        Assert.assertTrue(currentSlaveId != masterServerId);
    }


    @Test
    public void failOnSlaveAndMasterWithAutoConnect() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&autoReconnect=true&failOnReadOnly=true", true);

        //search actual server_id for master and slave
        int masterServerId = getServerId(connection);
        log.fine("master server_id = " + masterServerId);

        connection.setReadOnly(true);

        int firstSlaveId = getServerId(connection);
        log.fine("slave1 server_id = " + firstSlaveId);

        stopProxy(masterServerId);
        stopProxy(firstSlaveId);

        //must reconnect to the second slave without error
        connection.createStatement().execute("SELECT 1");
        int currentSlaveId = getServerId(connection);
        log.fine("currentSlaveId server_id = " + currentSlaveId);
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
    public void checkReconnectionToMasterAfterQueryNumber() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&secondsBeforeRetryMaster=3000&queriesBeforeRetryMaster=10&failOnReadOnly=true", true);
        Statement st = connection.createStatement();
        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);
        try {
            st.execute("SELECT 1");
        } catch (SQLException e) {
            Assert.fail();
        }
        Assert.assertTrue(connection.isReadOnly());

        restartProxy(masterServerId);
        long stoppedTime = System.currentTimeMillis();

        //not in autoreconnect mode, so must wait for query more than queriesBeforeRetryMaster
        for (int i = 1; i < 10; i++) {
            try {
                st.execute("SELECT 1");
                log.fine("i=" + i);
                Assert.assertTrue(connection.isReadOnly());
            } catch (SQLException e) {
                Assert.fail();
            }
        }

        boolean loop = true;
        while (loop) {
            try {
                Thread.sleep(250);
                st.execute("SELECT 1");
                if (!connection.isReadOnly()) {
                    log.fine("reconnection with failover loop after : " + (System.currentTimeMillis() - stoppedTime) + "ms");
                    loop = false;
                }
            } catch (SQLException e) {
                log.fine("not reconnected ... ");
            }
            if (System.currentTimeMillis() - stoppedTime > 20 * 1000) Assert.fail();
        }
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
        }
    }

    @Test
    public void checkBackOnMasterOnSlaveFail() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&secondsBeforeRetryMaster=1&failOnReadOnly=true", true);
        Statement st = connection.createStatement();
        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);

        try {
            st.execute("SELECT 1");
            Assert.assertTrue(connection.isReadOnly());
        } catch (SQLException e) {
            Assert.fail();
        }

        long stoppedTime = System.currentTimeMillis();
        restartProxy(masterServerId);
        boolean loop = true;
        while (loop) {
            Thread.sleep(250);
            try {
                if (!connection.isReadOnly()) {
                    log.fine("reconnection to master with failover loop after : " + (System.currentTimeMillis() - stoppedTime) + "ms");
                    loop = false;
                }
            } catch (SQLException e) {
            }
            if (System.currentTimeMillis() - stoppedTime > 15 * 1000) Assert.fail();
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
            Assert.fail();
        } catch (SQLException e) {
        }
    }

    @Test
    public void failoverMasterWithAutoConnectAndTransaction() throws Throwable {
        connection = getNewConnection("&autoReconnect=true&retriesAllDown=1", true);
        Statement st = connection.createStatement();

        int masterServerId = getServerId(connection);
        st.execute("drop table  if exists multinodeTransaction");
        st.execute("create table multinodeTransaction (id int not null primary key , amount int not null) ENGINE = InnoDB");
        connection.setAutoCommit(false);
        st.execute("insert into multinodeTransaction (id, amount) VALUE (1 , 100)");
        stopProxy(masterServerId);
        Assert.assertTrue(inTransaction(connection));
        try {
            // will to execute the query. if there is a connection error, try a ping, if ok, good, query relaunched. If not good, transaction is considered be lost
            st.execute("insert into multinodeTransaction (id, amount) VALUE (2 , 10)");
            Assert.fail();
        } catch (SQLException e) {
            log.finest("normal error : " + e.getMessage());
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
        connection = getNewConnection("&retriesAllDown=1&autoReconnect=true&failOnReadOnly=false", true);
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

    @Test
    public void testAutoReconnectMasterFailSlave() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1&autoReconnect=true&failOnReadOnly=true", true);
        Statement stmt = connection.createStatement();
        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);
        try {
            stmt.execute("SELECT 1");
        } catch (SQLException e) {
            Assert.fail();
        }
        Assert.assertTrue(connection.isReadOnly());
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



    /**
     * CONJ-79
     *
     * @throws SQLException
     */
    @Test
    public void socketTimeoutTest() throws SQLException {
        int exceptionCount = 0;
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
        } catch (Exception e){
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
//    @Test
//    public void testFailoverMaster() throws Throwable {
//        connection = getNewConnection("&validConnectionTimeout=1&secondsBeforeRetryMaster=1&autoReconnect=true", false);
//        Statement stmt = connection.createStatement();
//        ResultSet rs;
//        stmt.execute("ALTER SYSTEM SIMULATE 100 PERCENT DISK FAILURE FOR INTERVAL 60 SECOND");
//        int initMaster = getServerId(connection);
//        log.fine("master is " + initMaster);
//        FailoverProxy proxy = getProtocolFromConnection(connection).getProxy();
//        log.fine("lock : "+proxy.lock.getReadHoldCount() + " " + proxy.lock.getWriteHoldCount());
//        long failInitTime = 0;
//        long launchInit = System.currentTimeMillis();
//        while (System.currentTimeMillis() - launchInit < 120000) {
//            try {
//                Thread.sleep(2000);
//                if (stmt.isClosed()) {
//                    stmt = connection.createStatement();
//                }
//                rs = stmt.executeQuery("show global variables like 'innodb_read_only'");
//
//                rs.next();
//
//                if (failInitTime != 0 && "OFF".equals(rs.getString(2))) {
//                    long endFailover = System.currentTimeMillis() - launchInit;
//                    Assert.assertTrue(initMaster != getServerId(connection));
//                    log.fine("End failover after " + endFailover + " new master is " + getServerId(connection));
//                    //wait 15s for others tests may not be disturb by replica restart
//                    Thread.sleep(15000);
//                    return;
//                }
//            } catch (SQLException e) {
//                log.fine("error : " + e.getMessage());
//                if (failInitTime == 0) {
//                    failInitTime = System.currentTimeMillis();
//                    log.fine("start failover master was " + getServerId(connection));
//                }
//            }
//        }
//        Assert.fail();
//    }
//
//    @Test
//    public void testFailoverMasterPing() throws Throwable {
//        connection = getNewConnection("&failOnReadOnly=false&validConnectionTimeout=2&autoReconnect=true", false);
//        Statement stmt = connection.createStatement();
//        stmt.execute("ALTER SYSTEM SIMULATE 100 PERCENT DISK FAILURE FOR INTERVAL 35 SECOND");
//        int initMaster = getServerId(connection);
//        log.fine("master is " + initMaster);
//        long launchInit = System.currentTimeMillis();
//        while (System.currentTimeMillis() - launchInit < 180000) {
//            Thread.sleep(250);
//            int currentMaster = getServerId(connection);
//
//            //master must change
//            if (currentMaster != initMaster) {
//                long endFailover = System.currentTimeMillis() - launchInit;
//                log.fine("Master automatically change after failover after " + endFailover + "ms. new master is " + currentMaster);
//
//                //wait 15s for others tests may not be disturb by replica restart
//                Thread.sleep(15000);
//                return;
//            } else {
//                log.fine("++++++++ping in testFailoverMasterPing ");
//            }
//        }
//        Assert.fail();
//    }
//
//
//    @Test
//    public void FailoverWithAutoMasterSet () throws Throwable {
//        connection = getNewConnection("&validConnectionTimeout=2&secondsBeforeRetryMaster=1", false);
//        int initMaster = getServerId(connection);
//        connection.setReadOnly(true);
//        int connection1SlaveId = getServerId(connection);
//        int connection2SlaveId = connection1SlaveId;
//        FailoverProxy proxy = getProtocolFromConnection(connection).getProxy();
//        Connection connection2 = null;
//        try {
//            while (connection2SlaveId == connection1SlaveId) {
//                connection2 = getNewConnection("&validConnectionTimeout=5&secondsBeforeRetryMaster=1", false);
//                connection2.setReadOnly(true);
//                connection2SlaveId = getServerId(connection2);
//                if (connection2SlaveId == connection1SlaveId) connection2.close();
//            }
//
//            log.fine("master is " + initMaster);
//            log.fine("connection 1 slave is " + connection1SlaveId);
//            log.fine("connection 2 slave is " + connection2SlaveId);
//
//            connection.setReadOnly(false);
//            connection2.setReadOnly(false);
//
//            // now whe know the master and every slave.
//            // one of those replica will become a master
//            // the goal of this test is to check that he become silently master.
//            Statement stmt = connection.createStatement();
//            stmt.execute("ALTER SYSTEM SIMULATE 100 PERCENT DISK FAILURE FOR INTERVAL 35 SECOND");
//
//            boolean validationConnection1 = false;
//            boolean validationConnection2 = false;
//
//            //this permit to launched a failover is less than 15s after and every second, ping will test that master is ok.
//            long launchInit = System.currentTimeMillis();
//            while (System.currentTimeMillis() - launchInit < 180000) {
//                Thread.sleep(250);
//
//                int currentMaster1 = getServerId(connection);
//                int currentMaster2 = getServerId(connection2);
//
//                //master must change
//                if (currentMaster1 != initMaster || currentMaster2 != initMaster) {
//                    //connection master 1 has changed
//                    if (!validationConnection1 && currentMaster1 != initMaster) {
//                        //master has changed
//                        if (currentMaster1 == connection1SlaveId) {
//                            //master was old replica -> check that old replica has changed
//                            try {
//                                connection.setReadOnly(true);
//                                int currentSlave = getServerId(connection);
//
//                                if (currentSlave != currentMaster1) {
//                                    Thread.sleep(15000); //give time to detect salve too
//                                    currentSlave = getServerId(connection);
//                                }
//
//                                Assert.assertTrue(currentSlave != currentMaster1);
//                                validationConnection1 = true;
//                            } catch (SQLException e) {}
//                        } else validationConnection1 = true;
//                    }
//
//                    //connection master 1 has changed
//                    if (!validationConnection2 && currentMaster2 != initMaster) {
//                        //master has changed
//                        if (currentMaster2 == connection2SlaveId) {
//                            //master was old replica -> check that old replica has changed
//                            try {
//                                connection2.setReadOnly(true);
//                                connection2.createStatement().execute("SELECT 1");
//                                int currentSlave = getServerId(connection2);
//
//                                if (currentSlave != currentMaster2) {
//                                    Thread.sleep(15000); //give time to detect salve too
//                                    currentSlave = getServerId(connection2);
//                                }
//
//                                Assert.assertTrue(currentSlave != currentMaster2);
//                                validationConnection2 = true;
//                            } catch (SQLException e) {}
//                        } else validationConnection2 = true;
//                    }
//                    if (validationConnection1 && validationConnection2) {
//                        //wait 15s for others tests may not be disturb by replica restart
//                        log.finest("validated");
//                        Thread.sleep(15000);
//                        return;
//                    }
//                }
//            }
//            Assert.assertTrue(validationConnection1 && validationConnection2);
//        } finally {
//            connection2.close();
//        }
//    }

}
