package org.mariadb.jdbc.failover;

import org.junit.Assume;
import org.junit.Test;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.MariaDbServerPreparedStatement;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public abstract class BaseReplication extends BaseMonoServer {

    @Test
    public void failoverSlaveToMasterPrepareStatement() throws Throwable {
        try (Connection connection = getNewConnection(
                "&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000&useBatchMultiSend=false", true)) {
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists replicationFailoverBinary" + jobId);
            stmt.execute("create table replicationFailoverBinary" + jobId + " (id int not null primary key auto_increment, test VARCHAR(10))");
            stmt.execute("insert into replicationFailoverBinary" + jobId + "(test) values ('Harriba !')");
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            //wait for table replication on slave
            Thread.sleep(200);

            //create another prepareStatement, to permit to verify that prepare id has changed
            connection.prepareStatement("SELECT ?");

            //prepareStatement on slave connection
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT test from replicationFailoverBinary" + jobId + " where id = ?");
            final long currentPrepareId = getPrepareResult((MariaDbServerPreparedStatement) preparedStatement).getStatementId();
            int slaveServerId = getServerId(connection);
            assertFalse(masterServerId == slaveServerId);
            //stop slave for a few seconds
            stopProxy(slaveServerId, 2000);

            //test failover
            preparedStatement.setInt(1, 1);
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            assertEquals("Harriba !", rs.getString(1));
            assertNotEquals(currentPrepareId, getPrepareResult((MariaDbServerPreparedStatement) preparedStatement).getStatementId());

            int currentServerId = getServerId(connection);

            assertTrue(masterServerId == currentServerId);
            assertFalse(connection.isReadOnly());
            Thread.sleep(2000);
            boolean hasReturnOnSlave = false;

            for (int i = 0; i < 10; i++) {
                Thread.sleep(1000);
                preparedStatement.setInt(1, 1);
                rs = preparedStatement.executeQuery();
                rs.next();
                assertEquals("Harriba !", rs.getString(1));

                currentServerId = getServerId(connection);
                if (currentServerId != masterServerId) {
                    hasReturnOnSlave = true;
                    assertTrue(connection.isReadOnly());
                    break;
                }
            }
            assertTrue("Prepare statement has not return on Slave",hasReturnOnSlave);
        }
    }

    @Test()
    public void failoverSlaveAndMasterRewrite() throws Throwable {
        try (Connection connection = getNewConnection(
                "&rewriteBatchedStatements=true&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true)) {
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            int firstSlaveId = getServerId(connection);

            stopProxy(masterServerId);
            //stop proxy for 2s
            stopProxy(firstSlaveId, 4000);

            try {
                Statement stmt = connection.createStatement();
                stmt.addBatch("DO 1");
                stmt.addBatch("DO 2");
                int[] resultData = stmt.executeBatch();
                int secondSlaveId = getServerId(connection);
                assertEquals("the 2 batch queries must have been executed when failover", 2, resultData.length);
                assertTrue(secondSlaveId != firstSlaveId && secondSlaveId != masterServerId);
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    @Test
    public void failoverSlaveToMaster() throws Throwable {
        try (Connection connection = getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true)) {
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            int slaveServerId = getServerId(connection);
            assertFalse(masterServerId == slaveServerId);
            stopProxy(slaveServerId);
            connection.createStatement().execute("SELECT 1");
            int currentServerId = getServerId(connection);

            assertTrue(masterServerId == currentServerId);
            assertFalse(connection.isReadOnly());
        }
    }

    @Test
    public void failoverDuringSlaveSetReadOnly() throws Throwable {
        try (Connection connection = getNewConnection("&socketTimeout=3000", true)) {
            connection.setReadOnly(true);
            int slaveServerId = getServerId(connection);

            stopProxy(slaveServerId, 2000);
            connection.setReadOnly(false);
            int masterServerId = getServerId(connection);

            assertFalse(slaveServerId == masterServerId);
            assertFalse(connection.isReadOnly());
        }
        Thread.sleep(2500); //for not interfering with other tests
    }

    @Test()
    public void failoverSlaveAndMasterWithoutAutoConnect() throws Throwable {
        try (Connection connection = getNewConnection("&retriesAllDown=20&connectTimeout=1000&socketTimeout=1000", true)) {
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            int firstSlaveId = getServerId(connection);

            stopProxy(masterServerId);
            stopProxy(firstSlaveId);

            try {
                //will connect to second slave that isn't stopped
                connection.createStatement().executeQuery("SELECT CONNECTION_ID()");
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    @Test
    public void reconnectSlaveAndMasterWithAutoConnect() throws Throwable {
        try (Connection connection = getNewConnection(
                "&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true)) {

            //search actual server_id for master and slave
            int masterServerId = getServerId(connection);

            connection.setReadOnly(true);

            int firstSlaveId = getServerId(connection);

            stopProxy(masterServerId);
            stopProxy(firstSlaveId);

            //must reconnect to the second slave without error
            connection.createStatement().execute("SELECT 1");
            int currentSlaveId = getServerId(connection);
            assertTrue(currentSlaveId != firstSlaveId);
            assertTrue(currentSlaveId != masterServerId);
        }
    }


    @Test
    public void failoverMasterWithAutoConnect() throws Throwable {
        try (Connection connection = getNewConnection(
                "&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true)) {
            int masterServerId = getServerId(connection);

            stopProxy(masterServerId, 250);
            //with autoreconnect, the connection must reconnect automatically
            int currentServerId = getServerId(connection);

            assertTrue(currentServerId == masterServerId);
            assertFalse(connection.isReadOnly());
        }
        Thread.sleep(500); //for not interfering with other tests
    }

    @Test
    public void writeToSlaveAfterFailover() throws Throwable {
        try (Connection connection = getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true)) {
            //if super user can write on slave
            Assume.assumeTrue(!hasSuperPrivilege(connection, "writeToSlaveAfterFailover"));
            Statement st = connection.createStatement();
            st.execute("drop table  if exists writeToSlave" + jobId);
            st.execute("create table writeToSlave" + jobId + " (id int not null primary key , amount int not null) ENGINE = InnoDB");
            st.execute("insert into writeToSlave" + jobId + " (id, amount) VALUE (1 , 100)");

            int masterServerId = getServerId(connection);

            stopProxy(masterServerId);
            try {
                st.execute("insert into writeToSlave" + jobId + " (id, amount) VALUE (2 , 100)");
                fail();
            } catch (SQLException e) {
                //normal exception
                restartProxy(masterServerId);
                st = connection.createStatement();
                st.execute("drop table if exists writeToSlave" + jobId);
            }
        }
    }

    @Test
    public void randomConnection() throws Throwable {
        Map<HostAddress, MutableInt> connectionMap = new HashMap<>();
        int masterId = -1;
        for (int i = 0; i < 20; i++) {
            try (Connection connection = getNewConnection(false)) {
                ;
                int serverId = getServerId(connection);
                if (i > 0) {
                    assertTrue(masterId == serverId);
                }
                masterId = serverId;
                connection.setReadOnly(true);
                HostAddress replicaHost = getServerHostAddress(connection);
                MutableInt count = connectionMap.get(replicaHost);
                if (count == null) {
                    connectionMap.put(replicaHost, new MutableInt());
                } else {
                    count.increment();
                }
            }
        }

        assertTrue(connectionMap.size() >= 2);
        for (HostAddress key : connectionMap.keySet()) {
            Integer connectionCount = connectionMap.get(key).get();
            assertTrue(connectionCount > 1);
        }

    }

    class MutableInt {

        private int value = 1; // note that we start at 1 since we're counting

        public void increment() {
            ++value;
        }

        public int get() {
            return value;
        }
    }

    @Test
    public void closeWhenInReconnectionLoop() throws Throwable {
        try (Connection connection = getNewConnection("&connectTimeout=1000&socketTimeout=1000", true)) {
            int masterId = getServerId(connection);
            connection.setReadOnly(true);
            //close all slave proxy
            stopProxyButParameter(masterId);

            //trigger the failover, so a failover thread is launched
            Statement stmt = connection.createStatement();
            stmt.execute("SELECT 1");

            //launch connection close during failover must not throw error
            Thread.sleep(200);
        }
    }

    @Test
    public void failoverSlaveToMasterFail() throws Throwable {
        try (Connection connection = getNewConnection("&connectTimeout=1000&socketTimeout=1000&retriesAllDown=6", true)) {

            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            int slaveServerId = getServerId(connection);
            assertTrue(slaveServerId != masterServerId);

            connection.setCatalog("mysql"); //to be sure there will be a query, and so an error when switching connection
            stopProxy(masterServerId);
            try {
                //must throw error
                connection.setReadOnly(false);
                fail();
            } catch (SQLException e) {
                //normal exception
            }
            restartProxy(masterServerId);
        }
    }

    @Test
    public void failoverDuringMasterSetReadOnly() throws Throwable {
        try (Connection connection = getNewConnection("&retriesAllDown=6", true)) {
            int masterServerId = -1;
            masterServerId = getServerId(connection);

            stopProxy(masterServerId);

            connection.setReadOnly(true);

            int slaveServerId = getServerId(connection);

            assertFalse(slaveServerId == masterServerId);
            assertTrue(connection.isReadOnly());
            restartProxy(masterServerId);
        }
    }
}
