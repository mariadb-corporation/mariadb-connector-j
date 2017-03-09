package org.mariadb.jdbc.failover;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public abstract class BaseMonoServer extends BaseMultiHostTest {

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
    public void relaunchWithoutError() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&connectTimeout=1000&socketTimeout=1000", true);
            Statement st = connection.createStatement();
            int masterServerId = getServerId(connection);
            long startTime = System.currentTimeMillis();
            stopProxy(masterServerId, 4000);
            try {
                st.execute("SELECT 1");
                if (System.currentTimeMillis() - startTime < 4 * 1000) {
                    fail("Auto-reconnection must have been done after 4000ms but was " + (System.currentTimeMillis() - startTime));
                }
            } catch (SQLException e) {
                fail("must not have thrown error");
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void relaunchWithErrorWhenInTransaction() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&connectTimeout=1000&socketTimeout=1000", true);
            Statement st = connection.createStatement();
            st.execute("drop table if exists baseReplicationTransaction" + jobId);
            st.execute("create table baseReplicationTransaction" + jobId + " (id int not null primary key auto_increment, test VARCHAR(10))");

            connection.setAutoCommit(false);
            st.execute("INSERT INTO baseReplicationTransaction" + jobId + "(test) VALUES ('test')");
            int masterServerId = getServerId(connection);
            st.execute("SELECT 1");
            long startTime = System.currentTimeMillis();
            stopProxy(masterServerId, 2000);
            try {
                st.execute("SELECT 1");
                fail("must have thrown error since in transaction that is lost");
            } catch (SQLException e) {
                assertEquals("error type not normal after " + (System.currentTimeMillis() - startTime) + "ms", "25S03", e.getSQLState());
            }
            st.execute("drop table if exists baseReplicationTransaction" + jobId);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void failoverRelaunchedWhenSelect() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&connectTimeout=1000&socketTimeout=1000&retriesAllDown=6", true);
            Statement st = connection.createStatement();

            final int masterServerId = getServerId(connection);
            st.execute("drop table if exists selectFailover" + jobId);
            st.execute("create table selectFailover" + jobId + " (id int not null primary key , amount int not null) "
                    + "ENGINE = InnoDB");
            stopProxy(masterServerId, 2);
            try {
                st.execute("SELECT * from selectFailover" + jobId);
            } catch (SQLException e) {
                e.printStackTrace();
                fail("must not have thrown error");
            }

            stopProxy(masterServerId, 2);
            try {
                st.execute("INSERT INTO selectFailover" + jobId + " VALUES (1,2)");
                fail("not have thrown error !");
            } catch (SQLException e) {
                restartProxy(masterServerId);
                assertEquals("error type not normal", "25S03", e.getSQLState());
            }
        } finally {
            if (connection != null) {
                connection.createStatement().execute("drop table if exists selectFailover" + jobId);
                connection.close();
            }
        }
    }


    @Test
    public void failoverRelaunchedWhenInTransaction() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&connectTimeout=1000&socketTimeout=1000&retriesAllDown=6", true);
            Statement st = connection.createStatement();

            final int masterServerId = getServerId(connection);
            st.execute("drop table if exists selectFailoverTrans" + jobId);
            st.execute("create table selectFailoverTrans" + jobId + " (id int not null primary key , amount int not null) "
                    + "ENGINE = InnoDB");
            connection.setAutoCommit(false);
            st.execute("INSERT INTO selectFailoverTrans" + jobId + " VALUES (0,0)");
            stopProxy(masterServerId, 2);
            try {
                st.execute("SELECT * from selectFailoverTrans" + jobId);
                fail("not have thrown error !");
            } catch (SQLException e) {
                assertEquals("error type not normal", "25S03", e.getSQLState());
            }

            stopProxy(masterServerId, 2);
            try {
                st.execute("INSERT INTO selectFailoverTrans" + jobId + " VALUES (1,2)");
                fail("not have thrown error !");
            } catch (SQLException e) {
                restartProxy(masterServerId);
                st.execute("drop table if exists selectFailoverTrans" + jobId);
                assertEquals("error type not normal", "25S03", e.getSQLState());
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void pingReconnectAfterRestart() throws Throwable {
        Connection connection = null;
        try {
            connection = getNewConnection("&connectTimeout=1000&socketTimeout=1000&retriesAllDown=6", true);
            Statement st = connection.createStatement();
            int masterServerId = getServerId(connection);
            stopProxy(masterServerId);

            try {
                st.execute("SELECT 1");
            } catch (SQLException e) {
                //normal exception
            }
            restartProxy(masterServerId);
            long restartTime = System.nanoTime();

            boolean loop = true;
            while (loop) {
                if (!connection.isClosed()) {
                    loop = false;
                }
                try {
                    connection.createStatement();
                } catch (SQLException ee) {

                }
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - restartTime);
                if (duration > 20 * 1000) {
                    fail("Auto-reconnection not done after " + duration);
                }
                Thread.sleep(250);
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
