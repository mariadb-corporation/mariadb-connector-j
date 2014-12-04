package org.mariadb.jdbc;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.sql.SQLPermission;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.junit.Test;

public class ConnectionTest extends BaseTest {
	
	/**
	 * CONJ-89
	 * @throws SQLException 
	 */
	@Test
	public void getPropertiesTest() throws SQLException {
		String params = "&useFractionalSeconds=true&allowMultiQueries=true&useCompression=false";
		setConnection(params);
		Properties properties = connection.getClientInfo();
		assertTrue(properties != null);
		assertTrue(properties.size() > 0);
		assertTrue(properties.getProperty("user").equalsIgnoreCase(mUsername));
		//assertTrue(properties.getProperty("password").equalsIgnoreCase(mPassword));
		assertTrue(properties.getProperty("useFractionalSeconds").equalsIgnoreCase("true"));
		assertTrue(properties.getProperty("allowMultiQueries").equalsIgnoreCase("true"));
		assertTrue(properties.getProperty("useCompression").equalsIgnoreCase("false"));
		assertEquals(mUsername, connection.getClientInfo("user"));
		//assertEquals(null, connection.getClientInfo("password"));
		assertEquals("true", connection.getClientInfo("useFractionalSeconds"));
		assertEquals("true", connection.getClientInfo("allowMultiQueries"));
		assertEquals("false", connection.getClientInfo("useCompression"));
	}
	
	/**
	 * CONJ-75
	 * Needs permission java.sql.SQLPermission "abort" or will be skipped
	 * @throws SQLException
	 */
	@Test
	public void abortTest() throws SQLException {
		Statement stmt = connection.createStatement();
		SQLPermission sqlPermission = new SQLPermission("callAbort");
		SecurityManager securityManager = new SecurityManager();
		if (securityManager != null && sqlPermission != null) {
			try {
			securityManager.checkPermission(sqlPermission);
			} catch (SecurityException se) {
				System.out.println("test 'abortTest' skipped  due to missing policy");
				return;
			}
		}
		Executor executor = new Executor() {
			@Override
			public void execute(Runnable command) {
				command.run();
			}
		};
		connection.abort(executor);
		assertTrue(connection.isClosed());
		try {
			stmt.executeQuery("SELECT 1");
			assertTrue(false);
		} catch (SQLException sqle) {
			assertTrue(true);
		} finally {
			stmt.close();
		}
	}
	
	
	/**
	 * CONJ-121: implemented Connection.getNetworkTimeout and Connection.setNetworkTimeout
	 * @throws SQLException
	 */
	@Test
	public void networkTimeoutTest() throws SQLException {
		int timeout = 1000;
		SQLPermission sqlPermission = new SQLPermission("setNetworkTimeout");
		SecurityManager securityManager = new SecurityManager();
		if (securityManager != null && sqlPermission != null) {
			try {
			securityManager.checkPermission(sqlPermission);
			} catch (SecurityException se) {
				System.out.println("test 'setNetworkTimeout' skipped  due to missing policy");
				return;
			}
		}
		Executor executor = new Executor() {
			@Override
			public void execute(Runnable command) {
				command.run();
			}
		};
		try {
			connection.setNetworkTimeout(executor, timeout);
		} catch (SQLException sqlex) {
			assertTrue(false);
		}
		try {
			int networkTimeout = connection.getNetworkTimeout();
			assertEquals(timeout, networkTimeout);
		} catch (SQLException sqlex) {
			assertTrue(false);
		}
		try {
			connection.createStatement().execute("select sleep(2)");
			fail("Network timeout is " + timeout/1000 + "sec, but slept for 2sec");
		} catch (SQLException sqlex) {
			assertTrue(connection.isClosed());
		}
	}

}
