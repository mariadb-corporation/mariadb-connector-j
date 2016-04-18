package org.mariadb.jdbc.failover;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mariadb.jdbc.MariaDbServerPreparedStatement;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class BaseReplication extends BaseMonoServer {

    @Test
    public void failoverSlaveToMasterPrepareStatement() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true);
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
            final int currentPrepareId = getPrepareResult((MariaDbServerPreparedStatement) preparedStatement).getStatementId();
            int slaveServerId = getServerId(connection);
            Assert.assertFalse(masterServerId == slaveServerId);
            //stop slave for a few seconds
            stopProxy(slaveServerId, 2000);

            //test failover
            preparedStatement.setInt(1, 1);
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            Assert.assertEquals("Harriba !", rs.getString(1));
            Assert.assertNotEquals(currentPrepareId, getPrepareResult((MariaDbServerPreparedStatement) preparedStatement).getStatementId());

            int currentServerId = getServerId(connection);

            Assert.assertTrue(masterServerId == currentServerId);
            Assert.assertFalse(connection.isReadOnly());
            Thread.sleep(2000);
            boolean hasReturnOnSlave = false;

            for (int i = 0; i < 10; i++) {
                Thread.sleep(1000);
                preparedStatement.setInt(1, 1);
                rs = preparedStatement.executeQuery();
                rs.next();
                Assert.assertEquals("Harriba !", rs.getString(1));

                currentServerId = getServerId(connection);
                if (currentServerId != masterServerId) {
                    hasReturnOnSlave = true;
                    Assert.assertTrue(connection.isReadOnly());
                    break;
                }
            }
            Assert.assertTrue("Prepare statement has not return on Slave",hasReturnOnSlave);

        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test()
    public void failoverSlaveAndMasterRewrite() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&rewriteBatchedStatements=true&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true);
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
                Assert.fail();
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void failoverSlaveToMaster() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true);
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            int slaveServerId = getServerId(connection);
            Assert.assertFalse(masterServerId == slaveServerId);
            stopProxy(slaveServerId);
            connection.createStatement().execute("SELECT 1");
            int currentServerId = getServerId(connection);

            Assert.assertTrue(masterServerId == currentServerId);
            Assert.assertFalse(connection.isReadOnly());
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
            connection = getNewConnection("&socketTimeout=3000", true);
            connection.setReadOnly(true);
            int slaveServerId = getServerId(connection);

            stopProxy(slaveServerId, 2000);
            connection.setReadOnly(false);
            int masterServerId = getServerId(connection);

            Assert.assertFalse(slaveServerId == masterServerId);
            Assert.assertFalse(connection.isReadOnly());
        } finally {
            Thread.sleep(2500); //for not interfering with other tests
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test()
    public void failoverSlaveAndMasterWithoutAutoConnect() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true);
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            int firstSlaveId = getServerId(connection);

            stopProxy(masterServerId);
            stopProxy(firstSlaveId);

            try {
                //will connect to second slave that isn't stopped
                connection.createStatement().executeQuery("SELECT CONNECTION_ID()");
            } catch (SQLException e) {
                Assert.fail();
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void reconnectSlaveAndMasterWithAutoConnect() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true);

            //search actual server_id for master and slave
            int masterServerId = getServerId(connection);

            connection.setReadOnly(true);

            int firstSlaveId = getServerId(connection);

            stopProxy(masterServerId);
            stopProxy(firstSlaveId);

            //must reconnect to the second slave without error
            connection.createStatement().execute("SELECT 1");
            int currentSlaveId = getServerId(connection);
            Assert.assertTrue(currentSlaveId != firstSlaveId);
            Assert.assertTrue(currentSlaveId != masterServerId);
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
            connection = getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true);
            int masterServerId = getServerId(connection);

            stopProxy(masterServerId, 250);
            //with autoreconnect, the connection must reconnect automatically
            int currentServerId = getServerId(connection);

            Assert.assertTrue(currentServerId == masterServerId);
            Assert.assertFalse(connection.isReadOnly());
        } finally {
            Thread.sleep(500); //for not interfering with other tests
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void writeToSlaveAfterFailover() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=6&connectTimeout=1000&socketTimeout=1000", true);
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
                Assert.fail();
            } catch (SQLException e) {
                //normal exception
                restartProxy(masterServerId);
                st = connection.createStatement();
                st.execute("drop table if exists writeToSlave" + jobId);
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void randomConnection() throws Throwable {
        Connection connection = null;
        Map<String, MutableInt> connectionMap = new HashMap<>();
        int masterId = -1;
        for (int i = 0; i < 20; i++) {
            try {
                connection = getNewConnection(false);
                int serverId = getServerId(connection);
                if (i > 0) {
                    Assert.assertTrue(masterId == serverId);
                }
                masterId = serverId;
                connection.setReadOnly(true);
                int replicaId = getServerId(connection);
                MutableInt count = connectionMap.get(String.valueOf(replicaId));
                if (count == null) {
                    connectionMap.put(String.valueOf(replicaId), new MutableInt());
                } else {
                    count.increment();
                }
            } finally {
                if (connection != null) {
                    if (connection != null) {
                        connection.close();
                    }
                }
            }
        }

        Assert.assertTrue(connectionMap.size() >= 2);
        for (String key : connectionMap.keySet()) {
            Integer connectionCount = connectionMap.get(key).get();
            Assert.assertTrue(connectionCount > 1);
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
        Connection connection = null;
        try {
            connection = getNewConnection("&connectTimeout=1000&socketTimeout=1000", true);
            int masterId = getServerId(connection);
            connection.setReadOnly(true);
            //close all slave proxy
            stopProxyButParameter(masterId);

            //trigger the failover, so a failover thread is launched
            Statement stmt = connection.createStatement();
            stmt.execute("SELECT 1");

            //launch connection close during failover must not throw error
            Thread.sleep(200);
            connection.close();
            connection = null;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void failoverSlaveToMasterFail() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&connectTimeout=1000&socketTimeout=1000&retriesAllDown=6", true);
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            int slaveServerId = getServerId(connection);
            Assert.assertTrue(slaveServerId != masterServerId);

            connection.setCatalog("mysql"); //to be sure there will be a query, and so an error when switching connection
            stopProxy(masterServerId);
            try {
                //must throw error
                connection.setReadOnly(false);
                Assert.fail();
            } catch (SQLException e) {
                //normal exception
            }
            restartProxy(masterServerId);
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
            int masterServerId = -1;
            connection = getNewConnection("&retriesAllDown=6", true);
            masterServerId = getServerId(connection);

            stopProxy(masterServerId);

            connection.setReadOnly(true);

            int slaveServerId = getServerId(connection);

            Assert.assertFalse(slaveServerId == masterServerId);
            Assert.assertTrue(connection.isReadOnly());
            restartProxy(masterServerId);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
