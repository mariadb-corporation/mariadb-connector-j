package org.mariadb.jdbc.multihost;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.theories.suppliers.TestedOn;
import org.mariadb.jdbc.internal.common.QueryException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MultiHostFailoverTest extends BaseMultiHostTest {

    @Test
    public void testMultiHostWriteOnMaster() throws SQLException {
        Assume.assumeTrue(multihostUrlOk);
        Connection connection = null;
        log.fine("testMultiHostWriteOnMaster begin");
        try {
            connection = getNewConnection();
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists multinode");
            stmt.execute("create table multinode (id int not null primary key auto_increment, test VARCHAR(10))");
            log.fine("testMultiHostWriteOnMaster OK");
        } finally {
            log.fine("testMultiHostWriteOnMaster done");
            if (connection != null) connection.close();
        }
    }

    @Test
    public void testMultiHostWriteOnSlave() throws SQLException {
        Assume.assumeTrue(multihostUrlOk);
        Connection connection = null;
        log.fine("testMultiHostWriteOnSlave begin");
        try {
            connection = getNewConnection();
            //if super user, has write to write on slaves
            Assume.assumeTrue(!hasSuperPrivilege(connection, "testMultiHostWriteOnSlave"));
            connection.setReadOnly(true);
            Statement stmt = connection.createStatement();
            Assert.assertTrue(connection.isReadOnly());
            stmt.execute("drop table  if exists multinodeFail");
            try {
                stmt.execute("create table multinodeFail (id int not null primary key auto_increment, test VARCHAR(10))");
                log.severe("ERROR - > must not be able to write on slave --> check if you database is start with --read-only");
                Assert.fail();
            } catch (SQLException e) { }
        } finally {
            log.fine("testMultiHostWriteOnMaster done");
            if (connection != null) connection.close();
        }
    }

    @Test
    public void testMultiHostReadOnSlave() throws SQLException {
        Assume.assumeTrue(multihostUrlOk);
        Connection connection = null;
        log.fine("testMultiHostReadOnSlave begin");
        try {
            connection = getNewConnection();
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists multinodeRead");
            stmt.execute("create table multinodeRead (id int not null primary key auto_increment, test VARCHAR(10))");

            connection.setReadOnly(true);
            ResultSet rs = stmt.executeQuery("Select count(*) from multinodeRead");
            Assert.assertTrue(rs.next());
        } finally {
            log.fine("testMultiHostReadOnSlave done");
            if (connection != null) connection.close();
        }
    }

    @Test
    public void failoverSlaveToMaster() throws SQLException {
        Assume.assumeTrue(multihostUrlOk);
        Connection connection = null;
        log.fine("failoverSlaveToMaster begin");
        try {
            connection = getNewConnection();
            connection.setReadOnly(true);
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'server_id'");
            rs.next();
            int salveServerId = rs.getInt(2);

            tcpProxies[1].restart(2000);

            rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'server_id'");
            rs.next();
            int masterServerId = rs.getInt(2);

            Assert.assertFalse(salveServerId == masterServerId);
            Assert.assertFalse(connection.isReadOnly());
        } finally {
            log.fine("failoverSlaveToMaster done");
            try {
                Thread.sleep(2000); //wait to not have problem with next test
            } catch (InterruptedException e) {
            }
            if (connection != null) connection.close();
        }
    }


    @Test(expected = SQLException.class)
    public void failoverSlaveAndMasterWithoutAutoConnect() throws SQLException {
        Assume.assumeTrue(multihostUrlOk);
        Connection connection = null;
        log.fine("failoverSlaveAndMasterWithoutAutoConnect begin");
        try {
            connection = getNewConnection();
            Statement st = connection.createStatement();
            connection.setReadOnly(true);
            tcpProxies[0].restart(2000);
            tcpProxies[1].restart(2000);

            //must throw an error, because not in autoreconnect Mode
            ResultSet rs = st.executeQuery("SELECT CONNECTION_ID()");
            rs.next();
        } finally {
            log.fine("failoverSlaveAndMasterWithoutAutoConnect done");
            try {
                Thread.sleep(2000); //wait to not have problem with next test
            } catch (InterruptedException e) {
            }
            if (connection != null) connection.close();
        }
    }

    @Test
    public void failoverSlaveAndMasterWithAutoConnect() throws Throwable {
        Assume.assumeTrue(multihostUrlOk);
        Connection connection = null;
        log.fine("failoverSlaveAndMasterWithAutoConnect begin");
        try {
            connection = getNewConnection("&autoReconnect=true");
            Statement st = connection.createStatement();

            //search actual server_id for master and slave
            ResultSet rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'server_id'");
            rs.next();
            int masterServerId = rs.getInt(2);
            log.fine("master server_id = " + masterServerId);
            connection.setReadOnly(true);
            rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'server_id'");
            rs.next();
            int firstSlaveId = rs.getInt(2);
            log.fine("slave1 server_id = " + firstSlaveId);

            tcpProxies[0].restart(3000);
            tcpProxies[1].restart(3000);

            //must reconnect to the second slave without error
            rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'server_id'");
            rs.next();
            int currentSlaveId = rs.getInt(2);
            log.fine("currentSlaveId server_id = " + currentSlaveId);
            rs.next();
            Assert.assertTrue(currentSlaveId != firstSlaveId);
            Assert.assertTrue(currentSlaveId != masterServerId);
        } finally {
            log.fine("failoverSlaveAndMasterWithAutoConnect done");
            try {
                Thread.sleep(3000); //wait to not have problem with next test
            } catch (InterruptedException e) {
            }
            if (connection != null) connection.close();
        }
    }
    @Test
    public void failoverMasterWithAutoConnect() throws SQLException, InterruptedException {
        Assume.assumeTrue(multihostUrlOk);
        Connection connection = null;
        log.fine("failoverMasterWithAutoConnect begin");
        try {
            connection = getNewConnection("&autoReconnect=true");
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'server_id'");
            rs.next();
            int masterServerId = rs.getInt(2);

            tcpProxies[0].restart(100);
            //with autoreconnect, the connection must reconnect automatically
            rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'server_id'");
            rs.next();
            int currentServerId = rs.getInt(2);

            Assert.assertTrue(currentServerId == masterServerId);
            Assert.assertFalse(connection.isReadOnly());
        } finally {
            log.fine("failoverMasterWithAutoConnect done");
            try {
                Thread.sleep(2000); //wait to not have problem with next test
            } catch (InterruptedException e) {
            }
            if (connection != null) connection.close();
        }
    }

    @Test
    public void checkReconnectionToMasterAfterTimeout() throws SQLException, NoSuchFieldException, InterruptedException {
        Assume.assumeTrue(multihostUrlOk);
        Connection connection = null;
        log.fine("checkReconnectionToMasterAfterTimeout begin");
        try {
            connection = getNewConnection("&secondsBeforeRetryMaster=1");
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'server_id'");
            rs.next();
            int masterServerId = rs.getInt(2);

            tcpProxies[0].restart(2000);
            try {
                st.execute("SELECT 1");
            } catch (Exception e) {
            }

            //wait for more than the 1s (secondsBeforeRetryMaster) timeout, to check that master is on
            Thread.sleep(3000);

            rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'server_id'");
            rs.next();
            int currentServerId = rs.getInt(2);

            Assert.assertTrue(currentServerId == masterServerId);
            Assert.assertFalse(connection.isReadOnly());
        } finally {
            log.fine("checkReconnectionToMasterAfterTimeout done");
            if (connection != null) connection.close();
        }
    }

    @Test
    public void checkReconnectionToMasterAfterQueryNumber() throws SQLException, NoSuchFieldException, InterruptedException {
        Assume.assumeTrue(multihostUrlOk);
        Connection connection = null;
        log.fine("checkReconnectionToMasterAfterQueryNumber begin");
        try {
            connection = getNewConnection("&autoReconnect=true&secondsBeforeRetryMaster=30&queriesBeforeRetryMaster=10");
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'server_id'");
            rs.next();
            int masterServerId = rs.getInt(2);
            tcpProxies[0].restart(2000);

            for (int i = 0; i < 10; i++) {
                rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'server_id'");
                rs.next();
                int currentServerId = rs.getInt(2);
                Assert.assertFalse(currentServerId == masterServerId);
                Thread.sleep(250);
            }

            Thread.sleep(500);
            rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'server_id'");
            rs.next();
            int currentServerId = rs.getInt(2);
            Assert.assertTrue(currentServerId == masterServerId);
        } finally {
            log.fine("checkReconnectionToMasterAfterQueryNumber done");
            if (connection != null) connection.close();
        }
    }

    @Test
    public void writeToSlaveAfterFailover() throws SQLException, InterruptedException {
        Assume.assumeTrue(multihostUrlOk);
        Connection connection = null;
        log.fine("writeToSlaveAfterFailover begin");
        try {
            connection = getNewConnection();
            //if super user can write on slave
            Assume.assumeTrue(!hasSuperPrivilege(connection, "writeToSlaveAfterFailover"));
            Statement st = connection.createStatement();
            st.execute("drop table  if exists multinode2");
            st.execute("create table multinode2 (id int not null primary key , amount int not null) ENGINE = InnoDB");
            st.execute("insert into multinode2 (id, amount) VALUE (1 , 100)");
            tcpProxies[0].restart(2000);
            try {
                st.execute("insert into multinode2 (id, amount) VALUE (2 , 100)");
                Assert.fail();
            } catch (SQLException e) { }
        } finally {
            log.fine("writeToSlaveAfterFailover done");
            Thread.sleep(2000);
            if (connection != null) connection.close();
        }

    }

    @Test
    public void checkReconnectionAfterInactivity() throws Throwable {
        Assume.assumeTrue(multihostUrlOk);
        Connection connection = null;
        log.fine("checkReconnectionAfterInactivity begin");
        try {
            connection = getNewConnection("&validConnectionTimeout=1&secondsBeforeRetryMaster=4");
            Statement st = connection.createStatement();
            st.execute("drop table  if exists multinodeTransaction1");
            st.execute("create table multinodeTransaction1 (id int not null primary key , amount int not null) ENGINE = InnoDB");

            tcpProxies[0].restart(3000);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }

            log.fine("must be on a slave connection");
            //must have failover to slave connection
            Assert.assertTrue(connection.isReadOnly());

            //wait for more than the 4s (secondsBeforeRetryMaster) timeout, to check that master is on
            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {
            }

            //must have found back the master
            log.fine("must be on the master connection");
            Assert.assertFalse(connection.isReadOnly());
            log.fine("checkReconnectionAfterInactivity done");

        } finally {
            log.fine("checkReconnectionAfterInactivity done");
            if (connection != null) connection.close();
        }
    }

    @Test(expected = SQLException.class)
    public void checkNoSwitchConnectionDuringTransaction() throws Throwable {
        Assume.assumeTrue(multihostUrlOk);
        Connection connection = null;
        log.fine("checkNoSwitchConnectionDuringTransaction begin");
        try {
            connection = getNewConnection("&autoReconnect=true");
            Statement st = connection.createStatement();

            st.execute("drop table  if exists multinodeTransaction2");
            st.execute("create table multinodeTransaction2 (id int not null primary key , amount int not null) ENGINE = InnoDB");
            connection.setAutoCommit(false);
            st.execute("insert into multinodeTransaction (id, amount) VALUE (1 , 100)");
            connection.setReadOnly(true);
        } finally {
            log.fine("checkNoSwitchConnectionDuringTransaction done");
            if (connection != null) connection.close();
        }
    }

    @Test
    public void failoverMasterWithAutoConnectAndTransaction() throws Throwable {
        Assume.assumeTrue(multihostUrlOk);
        Connection connection = null;
        log.fine("failoverMasterWithAutoConnectAndTransaction begin");
        try {
            connection = getNewConnection("&autoReconnect=true");
            //if super user, will write to slave
            Assume.assumeTrue(!hasSuperPrivilege(connection, "failoverMasterWithAutoConnectAndTransaction"));
            Statement st = connection.createStatement();

            st.execute("drop table  if exists multinodeTransaction");
            st.execute("create table multinodeTransaction (id int not null primary key , amount int not null) ENGINE = InnoDB");
            connection.setAutoCommit(false);
            st.execute("insert into multinodeTransaction (id, amount) VALUE (1 , 100)");
            st.execute("insert into multinodeTransaction (id, amount) VALUE (2 , 200)");
            st.execute("START TRANSACTION");
            st.execute("update multinodeTransaction set amount = amount+100");
            st.execute("insert into multinodeTransaction (id, amount) VALUE (3 , 10)");

            tcpProxies[0].restart(500);
            try {
                //with autoreconnect but in transaction, query must throw an error
                st.execute("insert into multinodeTransaction (id, amount) VALUE (4 , 10)");
                Assert.fail();
            } catch (SQLException e) { }
        } finally {
            log.fine("failoverMasterWithAutoConnectAndTransaction done");
            Thread.sleep(2000); //wait to not have problem with next test
            if (connection != null) {
                try {
                    connection.setAutoCommit(true);
                }
                catch (SQLException e) {}
                connection.close();
            }
        }
    }

    @Test
    public void testSynchronizedReadOnly() throws SQLException, InterruptedException {
        Assume.assumeTrue(multihostUrlOk);
        Connection connection = null;
        log.fine("testSynchronizedReadOnly begin");
        try {
            connection = getNewConnection();
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists multisync");
            stmt.execute("create table multisync (id int not null primary key , amount int not null) ENGINE = InnoDB");
            stmt.execute("INSERT INTO multisync (id, amount) values (1, 0)");
            long currentTime = System.currentTimeMillis();
            ExecutorService exec= Executors.newFixedThreadPool(2);
            exec.execute(new ChangeAmount(connection, 100));
            exec.execute(new ChangeAmount(connection, 100));
            //wait for thread endings
            exec.shutdown();
            try {
                exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) { }
            log.info("total time : "+ (System.currentTimeMillis() - currentTime));
            ResultSet rs = stmt.executeQuery("SELECT amount FROM multisync");
            rs.next();
            log.fine(" total result :" + rs.getInt(1));
            Assert.assertTrue(200 == rs.getInt(1));
        } finally {
            log.fine("testSynchronizedReadOnly done");
            if (connection != null) connection.close();
        }
    }

    protected class ChangeAmount implements Runnable {
        Connection connection;
        int changeAmount;
        public ChangeAmount(Connection connection, int changeAmount) {
            this.connection = connection;
            this.changeAmount = changeAmount;
        }

        public void run() {
            try {
                Statement st = connection.createStatement();
                for (int i = 1; i <= changeAmount; i++) {
                    st.execute("UPDATE  multisync set amount = amount + 1");
                    if (i%200==0)log.fine("update : "+i);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testSyncReadOnly() throws SQLException, InterruptedException {
        Assume.assumeTrue(multihostUrlOk);
        Connection connection = null;
        log.fine("testSyncReadOnly begin");
        try {
            connection = getNewConnection();
            Assert.assertFalse(connection.isReadOnly());

            long currentTime = System.currentTimeMillis();
            ExecutorService exec= Executors.newFixedThreadPool(2);
            exec.execute(new ChangeReadOnly(connection, 100));
            exec.execute(new ChangeReadOnly(connection, 100));
            //wait for thread endings
            exec.shutdown();
            try {
                exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) { }
            log.info("total time : "+ (System.currentTimeMillis() - currentTime));
        } finally {
            log.fine("testSyncReadOnly done");
            if (connection != null) connection.close();
        }
    }

    protected class ChangeReadOnly implements Runnable {
        Connection connection;
        int changeAmount;

        public ChangeReadOnly(Connection connection, int changeAmount) {
            this.connection = connection;
            this.changeAmount = changeAmount;
        }

        public void run() {
            try {
                for (int i = 1; i <= changeAmount; i++) {
                    connection.setReadOnly(!connection.isReadOnly());
                    if (i%20==0) {
                        log.fine("setReadOnly change :"+i);
                        Assert.assertFalse(connection.isReadOnly());
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
