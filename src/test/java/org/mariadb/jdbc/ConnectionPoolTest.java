package org.mariadb.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Test;

public class ConnectionPoolTest extends BaseTest {
    
    /* For this test case to compile the following must be added to the pom.xml:
       <dependency>
         <groupId>commons-dbcp</groupId>
         <artifactId>commons-dbcp</artifactId>
         <version>1.4</version>
      </dependency>
     */
    @Test
    public void testConnectionWithApacheDBCP() throws SQLException {
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
    
    /*
     * 
     */
    @Test
    public void testTimeoutsInPool() throws SQLException, InterruptedException {
        org.apache.commons.dbcp.BasicDataSource dataSource;
        dataSource = new org.apache.commons.dbcp.BasicDataSource();
        dataSource.setUrl("jdbc:mysql://" + hostname + ":3306/test?useCursorFetch=true&useTimezone=true&useLegacyDatetimeCode=false&serverTimezone=UTC");
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        // dataSource.setMaxActive(10);
        // dataSource.setMinIdle(10); //keep 10 connections open
        // dataSource.setValidationQuery("SELECT 1");
        dataSource.setMaxActive(50);
        dataSource.setLogAbandoned(true);
        dataSource.setRemoveAbandoned(true);
        dataSource.setRemoveAbandonedTimeout(300);
        dataSource.setAccessToUnderlyingConnectionAllowed(true);
        dataSource.setMinEvictableIdleTimeMillis(1800000);
        dataSource.setTimeBetweenEvictionRunsMillis(-1);
        dataSource.setNumTestsPerEvictionRun(3);
        
        // adjust server wait timeout to 1 second
        // Statement stmt1 = conn1.createStatement();
        // stmt1.execute("set session wait_timeout=1");
        
        
        try {
    		Connection conn = dataSource.getConnection();
    		System.out.println("autocommit: " + conn.getAutoCommit());
        	Statement stmt = conn.createStatement();
        	stmt.executeUpdate("drop table if exists t3");
    		stmt.executeUpdate("create table t3(message text)");
        	conn.close();
    	} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        
        InsertThread ins1 = new InsertThread(10000, dataSource);
        Thread thread1 = new Thread(ins1);
        thread1.start();
        InsertThread ins2 = new InsertThread(10000, dataSource);
        Thread thread2 = new Thread(ins2);
        thread2.start();
        InsertThread ins3 = new InsertThread(10000, dataSource);
        Thread thread3 = new Thread(ins3);
        thread3.start();
        InsertThread ins4 = new InsertThread(10000, dataSource);
        Thread thread4 = new Thread(ins4);
        thread4.start();
        InsertThread ins5 = new InsertThread(10000, dataSource);
        Thread thread5 = new Thread(ins5);
        thread5.start();
        InsertThread ins6 = new InsertThread(10000, dataSource); 
        Thread thread6 = new Thread(ins6);
        thread6.start();
        InsertThread ins7 = new InsertThread(10000, dataSource);
        Thread thread7 = new Thread(ins7);
        thread7.start();
        InsertThread ins8 = new InsertThread(10000, dataSource);
        Thread thread8 = new Thread(ins8);
        thread8.start();
        InsertThread ins9 = new InsertThread(10000, dataSource);
        Thread thread9 = new Thread(ins9);
        thread9.start();
        InsertThread ins10 = new InsertThread(10000, dataSource);
        Thread thread10 = new Thread(ins10);
        thread10.start();
                
        // wait for threads to finish
        while (thread1.isAlive() || thread2.isAlive() || thread3.isAlive() || thread4.isAlive() || thread5.isAlive() || thread6.isAlive() || thread7.isAlive() || thread8.isAlive() || thread9.isAlive() || thread10.isAlive())
        {
        	//keep on waiting for threads to finish
        }
        
        // wait for 70 seconds so that the server times out the connections
        Thread.sleep(70000); // Wait for the server to kill the connections
        
        // do something
     	Statement stmt1 = dataSource.getConnection().createStatement();
        stmt1.execute("SELECT COUNT(*) FROM t3");
        
        // close data source
        dataSource.close();
        
        /*
        Connection conn1 = null;
        Statement stmt1 = null;
        ResultSet rs;
        
        for(int i = 1; i < 100000; i++)
        {
        	conn1 = dataSource.getConnection();
        	stmt1 = conn1.createStatement();
			rs = stmt1.executeQuery("SELECT 1");	
			rs.next();
			conn1.close();
        }
        */
        
        // close all connections but conn1
        /* conn1.close();
        conn2.close();
        conn3.close();
        conn4.close();
        conn5.close();
        */
        // dataSource.close();
    }
    
    /**
     * This test case simulates how the Apache DBCP connection pools works. It is written so it
     * should compile without Apache DBCP but still show the problem.
     */
    @Test
    public void testConnectionWithSimululatedApacheDBCP() throws SQLException {
        
        java.sql.Driver driver = new org.mariadb.jdbc.Driver();

        Properties props = new Properties();
        props.put("user", username);
        props.put("password", password);
        
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
    
    /** This class is a simulated version of org.apache.commons.dbcp.DriverConnectionFactory */
    private static class SimulatedDriverConnectionFactory {
        public SimulatedDriverConnectionFactory(java.sql.Driver driver, String connectUri, Properties props) {
            _driver = driver;
            _connectUri = connectUri;
            _props = props;
        }

        public Connection createConnection() throws SQLException {
            return _driver.connect(_connectUri,_props);
        }

        protected java.sql.Driver _driver = null;
        protected String _connectUri = null;
        protected Properties _props = null;
    }
    
    private class InsertThread implements Runnable {
    	private org.apache.commons.dbcp.BasicDataSource dataSource;
    	private int insertTimes;

        public InsertThread(int insertTimes, org.apache.commons.dbcp.BasicDataSource dataSource) {
            this.insertTimes = insertTimes;
            this.dataSource = dataSource;
        }

        public synchronized void setDataSource(org.apache.commons.dbcp.BasicDataSource dataSource) {
            this.dataSource = dataSource;
        }

        public synchronized org.apache.commons.dbcp.BasicDataSource getDataSource() {
            return this.dataSource;
        }

        public void run() {
        	Connection conn = null;
        	Statement stmt = null;
            
            for(int i = 1; i < insertTimes + 1; i++)
            {
    			try {
    				conn = this.dataSource.getConnection();
    				stmt = conn.createStatement();
    				stmt.execute("insert into t3 values('hello" + Thread.currentThread().getId() + "-" + i + "')"); 
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					System.out.println(e.getSQLState());
				}
    			System.out.println("thread: " + Thread.currentThread().getId());
            }
        }
    }
}
