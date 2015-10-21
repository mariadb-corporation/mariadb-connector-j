package org.mariadb.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class ConnectionPoolTest extends BaseTest {

    @Test
    public void testConnectionWithApacheDbcp() throws SQLException {
        org.apache.commons.dbcp.BasicDataSource dataSource;
        dataSource = new org.apache.commons.dbcp.BasicDataSource();
        dataSource.setUrl(connU);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaxActive(5);
        dataSource.setLogAbandoned(true);
        dataSource.setRemoveAbandoned(true);
        dataSource.setRemoveAbandonedTimeout(300);
        dataSource.setAccessToUnderlyingConnectionAllowed(true);
        dataSource.setMinEvictableIdleTimeMillis(1800000);
        dataSource.setTimeBetweenEvictionRunsMillis(-1);
        dataSource.setNumTestsPerEvictionRun(3);

        // dataSource.setValidationQuery("/* ping */ SELECT 1");

        Connection connection = dataSource.getConnection();

        connection.close();
        dataSource.close();
    }


    /**
     * This test case simulates how the Apache DBCP connection pools works. It is written so it
     * should compile without Apache DBCP but still show the problem.
     */
    @Test
    public void testConnectionWithSimululatedApacheDbcp() throws SQLException {

        java.sql.Driver driver = new org.mariadb.jdbc.Driver();

        Properties props = new Properties();
        props.put("user", username);
        props.put("password", (password == null) ? "" : password);

        //A connection pool typically has a connection factor that stored everything needed to
        //create a Connection. Here I create a factory that stores URL, username and password.
        SimulatedDriverConnectionFactory factory = new SimulatedDriverConnectionFactory(driver,
                connU, props);

        //Create 1 first connection (This is typically done in the Connection validation step in a
        //connection pool)
        Connection connection1 = factory.createConnection();

        //Create another connection to make sure we can access the database. This is typically the
        //Connection that is exposed to the user of the connection pool
        Connection connection2 = factory.createConnection();

        connection1.close();
        connection2.close();
    }

    /**
     * This class is a simulated version of org.apache.commons.dbcp.DriverConnectionFactory
     */
    private static class SimulatedDriverConnectionFactory {
        protected java.sql.Driver internalDriver = null;
        protected String internalConnectUri = null;
        protected Properties internalProps = null;

        public SimulatedDriverConnectionFactory(java.sql.Driver driver, String connectUri, Properties props) {
            internalDriver = driver;
            internalConnectUri = connectUri;
            internalProps = props;
        }

        public Connection createConnection() throws SQLException {
            return internalDriver.connect(internalConnectUri, internalProps);
        }
    }

    private class InsertThread implements Runnable {
        private org.apache.commons.dbcp.BasicDataSource dataSource;
        private int insertTimes;

        public InsertThread(int insertTimes, org.apache.commons.dbcp.BasicDataSource dataSource) {
            this.insertTimes = insertTimes;
            this.dataSource = dataSource;
        }

        public synchronized org.apache.commons.dbcp.BasicDataSource getDataSource() {
            return this.dataSource;
        }

        public synchronized void setDataSource(org.apache.commons.dbcp.BasicDataSource dataSource) {
            this.dataSource = dataSource;
        }

        public void run() {
            Connection conn = null;
            Statement stmt = null;

            for (int i = 1; i < insertTimes + 1; i++) {
                try {
                    conn = this.dataSource.getConnection();
                    stmt = conn.createStatement();
                    stmt.execute("insert into t3 values('hello" + Thread.currentThread().getId() + "-" + i + "')");
                    conn.close();
                } catch (SQLException e) {
                    log.debug(e.getSQLState());
                }
            }
        }
    }
}
