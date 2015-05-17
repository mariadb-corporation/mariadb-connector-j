package org.mariadb.jdbc;

import static org.junit.Assert.*;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FailoverTest {
	static { Logger.getLogger("").setLevel(Level.FINEST); }
	private final static Logger log = Logger.getLogger(FailoverTest.class.getName());

	//the active connection
	protected Connection connection;
	//default multi-host URL
	protected static final String defaultUrl = "jdbc:mysql://host1,host2,host3:3306/test?user=root";
	//hosts
	protected String[] hosts;

	@Before
	public void beforeClassFailover()  throws SQLException {
		//get the multi-host connection string
		String url = System.getProperty("dbUrl", defaultUrl);
		//parse the url
    	JDBCUrl jdbcUrl = JDBCUrl.parse(url);
		connection = DriverManager.getConnection(url);
	}


	@Test
	public void simulateConnectingToFirstHost() throws SQLException {
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery("show global variables like 'innodb_read_only'");
		rs.next();
		Assert.assertTrue("OFF".equals(rs.getString(2)));
	}
	
	@Test
	public void simulateFailingFirstHost()  throws SQLException{
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery("show global variables like 'innodb_read_only'");
		rs.next();
		log.info("READ ONLY : " + rs.getString(2));
		Assert.assertTrue("OFF".equals(rs.getString(2)));

		//simulate master crash
		try {
			stmt.execute("ALTER SYSTEM CRASH");
		} catch ( Exception e) { }

		//verification that secondary take place
		rs = stmt.executeQuery("show global variables like 'innodb_read_only'");
		rs.next();
		log.info("READ ONLY : " + rs.getString(2));
		Assert.assertTrue("ON".equals(rs.getString(2)));
	}

	@Test
	public void simulateTwoFirstHostsDown() {
		fail("Not implemented");
	}
	
	@Test
	public void checkSessionInfoWithConnectionChange() {
		fail("Not implemented");
	}

	@Test
	public void simulateAllHostsDown() {
		fail("Not implemented");
	}


	@Test
	public void loadBalance() {
		fail("Not implemented");
	}
	
    @After
    public void after() throws SQLException {
        try {
        	connection.close();
        } catch(Exception e) {
        	logInfo(e.toString());
        }
    }

    // common function for logging information 
    static void logInfo(String message) {
		log.info(message);
    }
}
