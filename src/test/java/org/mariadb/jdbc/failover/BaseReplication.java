package org.mariadb.jdbc.failover;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseReplication extends BaseMultiHostTest {

    @Test
    public void testWriteOnMaster() throws SQLException {
        Connection connection = null;
        try {
            connection = getNewConnection(false);
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists auroraMultiNode" + jobId);
            stmt.execute("create table auroraMultiNode" + jobId + " (id int not null primary key auto_increment, test VARCHAR(10))");
            stmt.execute("drop table  if exists auroraMultiNode" + jobId);
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
            connection = getNewConnection("&retriesAllDown=1", true);
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
            connection = getNewConnection(true);
            connection.setReadOnly(true);
            int slaveServerId = getServerId(connection);

            stopProxy(slaveServerId, 2000);
            connection.setReadOnly(false);
            int masterServerId = getServerId(connection);

            Assert.assertFalse(slaveServerId == masterServerId);
            Assert.assertFalse(connection.isReadOnly());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test()
    public void failoverSlaveAndMasterWithoutAutoConnect() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&retriesAllDown=1", true);
            int masterServerId = getServerId(connection);
            connection.setReadOnly(true);
            int firstSlaveId = getServerId(connection);

            stopProxy(masterServerId);
            stopProxy(firstSlaveId);

            try {
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
            connection = getNewConnection("&retriesAllDown=1&autoReconnect=true", true);

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
            connection = getNewConnection("&retriesAllDown=1&autoReconnect=true", true);
            int masterServerId = getServerId(connection);

            stopProxy(masterServerId, 250);
            //with autoreconnect, the connection must reconnect automatically
            int currentServerId = getServerId(connection);

            Assert.assertTrue(currentServerId == masterServerId);
            Assert.assertFalse(connection.isReadOnly());
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
                st.execute("drop table  if exists writeToSlave" + jobId);
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

            st.execute("drop table  if exists multinodeTransaction2_" + jobId);
            st.execute("create table multinodeTransaction2_" + jobId + " (id int not null primary key , amount int not null) "
                    + "ENGINE = InnoDB");
            connection.setAutoCommit(false);
            st.execute("insert into multinodeTransaction2_" + jobId + " (id, amount) VALUE (1 , 100)");

            try {
                //in transaction, so must trow an error
                connection.setReadOnly(true);
                Assert.fail();
            } catch (SQLException e) {
                //normal exception
                connection.setReadOnly(false);
                st.execute("drop table  if exists multinodeTransaction2_" + jobId);
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
            connection = getNewConnection(true);
            connection.setReadOnly(true);
            int serverId = getServerId(connection);
            stopProxy(serverId);

            //trigger the failover, so a failover thread is launched
            Statement stmt = connection.createStatement();
            stmt.execute("SELECT 1");
            //launch connection close
            new CloseConnection(connection).run();
        } finally {
            if (connection != null) {
                if (connection != null) {
                    connection.close();
                }
            }
        }

    }

    protected class CloseConnection implements Runnable {
        Connection connection;

        public CloseConnection(Connection connection) {
            this.connection = connection;
        }

        public void run() {
            try {
                Thread.sleep(2400); // wait that slave reconnection loop is launched
                connection.close();
            } catch (Throwable e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
    }
}
