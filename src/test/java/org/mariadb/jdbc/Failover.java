package org.mariadb.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class Failover {
	//the active connection
	protected Connection connection;
	//default multi-host URL
	protected static final String defaultUrl = "jdbc:mysql://host1,host2,host3:3306/test?user=root";
	//hosts
	protected String[] hosts;

	@BeforeClass
	public static void beforeClassFailover() {
		//get the multi-host connection string
		String url = System.getProperty("dbUrl", defaultUrl);
		//parse the url
		//TODO JDBCUrl cannot parse the multi-hosts
    	JDBCUrl jdbcUrl = JDBCUrl.parse(url);
    	//TODO store hosts in list
    	
    	//TODO add support for the following url properties
    	//autoReconnect, maxReconnects, queriesBeforeRetryMaster, secondsBeforeRetryMaster
	}
	
	@Before
    public void before() throws SQLException{
        //get the new multi-host connection
		//TODO use DataSource
		//TODO MySQLConnection need to support multi-host
    }
	
	@Test
	public void simulateConnectingToFirstHost()
	{
		fail("Not implemented");
	}
	
	@Test
	public void simulateFailingFirstHost()
	{
		fail("Not implemented");
	}
	
	@Test
	public void simulateTwoFirstHostsDown()
	{
		fail("Not implemented");
	}
	
	@Test
	public void simulateAllHostsDown()
	{
		fail("Not implemented");
	}
	
	@Test
	public void loadBalance()
	{
		fail("Not implemented");
	}
	
    @After
    public void after() throws SQLException {
        try
        {
        	connection.close();
        }
        	catch(Exception e)
        {
        	logInfo(e.toString());
        }
    }

    // common function for logging information 
    static void logInfo(String message)
    {
    	System.out.println(message);
    }
}
