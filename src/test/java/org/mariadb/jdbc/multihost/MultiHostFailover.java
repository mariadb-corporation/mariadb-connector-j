package org.mariadb.jdbc.multihost;

import org.junit.Assert;
import org.junit.Test;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.MySQLConnection;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MultiHostFailover extends BaseMultiHostTest {

    @Test
    public void testMultiHostWriteOnMaster() throws SQLException {
        Connection connection = null;
        try {
            connection = getNewConnection();
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists multinode");
            stmt.execute("create table multinode (id int not null primary key auto_increment, test VARCHAR(10))");
            log.fine("testMultiHostWriteOnMaster OK");
        } finally {
            connection.close();
        }
    }

    @Test(expected = SQLException.class)
    public void testMultiHostWriteOnSlave() throws SQLException {
        Connection connection = null;
        try {
            connection = getNewConnection();
            connection.setReadOnly(true);
            Statement stmt = connection.createStatement();
            Assert.assertTrue(connection.isReadOnly());
            stmt.execute("drop table  if exists multinodeFail");
            stmt.execute("create table multinodeFail (id int not null primary key auto_increment, test VARCHAR(10))");

            log.severe("ERROR - > must not be able to write on slave --> check if you database is start with --read-only");
            Assert.assertTrue(false);
        } finally {
            log.fine("testMultiHostWriteOnMaster done");
            connection.close();
        }
    }

    @Test
    public void testMultiHostReadOnSlave() throws SQLException {
        Connection connection = null;
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
            connection.close();
        }
    }

    @Test
    public void failoverSlaveToMaster() throws SQLException {
        Connection connection = null;
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
            connection.close();
            try {
                Thread.sleep(2000); //wait to not have problem with next test
            } catch (InterruptedException e) {
            }
        }
    }


    @Test(expected = SQLException.class)
    public void failoverSlaveAndMasterWithoutAutoConnect() throws SQLException {
        Connection connection = null;
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
            connection.close();
            try {
                Thread.sleep(2000); //wait to not have problem with next test
            } catch (InterruptedException e) {
            }
        }
    }

    @Test
    public void failoverSlaveAndMasterWithAutoConnect() throws Throwable {
        Connection connection = null;
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
            connection.close();
            try {
                Thread.sleep(3000); //wait to not have problem with next test
            } catch (InterruptedException e) {
            }
        }
    }
    @Test
    public void failoverMasterWithAutoConnect() throws SQLException, InterruptedException{
        Connection connection = null;
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
            connection.close();
            try {
                Thread.sleep(2000); //wait to not have problem with next test
            } catch (InterruptedException e) {
            }
        }
    }

    @Test
    public void checkReconnectionToMasterAfterTimeout() throws SQLException, NoSuchFieldException, InterruptedException {
        Connection connection = null;
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
            connection.close();
        }
    }

    @Test
    public void checkReconnectionToMasterAfterQueryNumber() throws SQLException, NoSuchFieldException, InterruptedException {
        Connection connection = null;
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
            connection.close();
        }
    }

    @Test(expected = SQLException.class)
    public void writeToSlaveAfterFailover() throws SQLException, InterruptedException{
        Connection connection = null;
        try {
            connection = getNewConnection();
            Statement st = connection.createStatement();
            st.execute("drop table  if exists multinode2");
            st.execute("create table multinode2 (id int not null primary key , amount int not null) ENGINE = InnoDB");
            st.execute("insert into multinode2 (id, amount) VALUE (1 , 100)");
            tcpProxies[0].restart(2000);

            st.execute("insert into multinode2 (id, amount) VALUE (1 , 100)");
            Assert.assertTrue(false);
        } finally {
            connection.close();
            Thread.sleep(2000);
        }

    }
    @Test
    public void checkReconnectionAfterInactivity() throws Throwable {
        Connection connection = null;
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
            connection.close();
        }
    }

    @Test(expected = SQLException.class)
    public void checkNoSwitchConnectionDuringTransaction() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&autoReconnect=true");
            Statement st = connection.createStatement();

            st.execute("drop table  if exists multinodeTransaction2");
            st.execute("create table multinodeTransaction2 (id int not null primary key , amount int not null) ENGINE = InnoDB");
            connection.setAutoCommit(false);
            st.execute("insert into multinodeTransaction (id, amount) VALUE (1 , 100)");
            connection.setReadOnly(true);
        } finally {
            connection.close();
        }
    }

    @Test(expected = SQLException.class)
    public void failoverMasterWithAutoConnectAndTransaction() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&autoReconnect=true");
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
            //with autoreconnect but in transaction, query must throw an error
            st.execute("insert into multinodeTransaction (id, amount) VALUE (3 , 10)");

        } finally {
            connection.close();
            Thread.sleep(2000); //wait to not have problem with next test
        }

    }
}
