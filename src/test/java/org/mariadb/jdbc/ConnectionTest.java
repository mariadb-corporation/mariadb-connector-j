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
		assertTrue(properties.getProperty("user") == null);
		assertTrue(properties.getProperty("password") == null);
		assertTrue(properties.getProperty("useFractionalSeconds").equalsIgnoreCase("true"));
		assertTrue(properties.getProperty("allowMultiQueries").equalsIgnoreCase("true"));
		assertTrue(properties.getProperty("useCompression").equalsIgnoreCase("false"));
		assertEquals(null, connection.getClientInfo("user"));
		assertEquals(null, connection.getClientInfo("password"));
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
		SQLPermission sqlPermission = new SQLPermission("abort");
		SecurityManager securityManager = new SecurityManager();
		if (securityManager != null && sqlPermission != null) {
			try {
			securityManager.checkPermission(sqlPermission);
			} catch (SecurityException se) {
				System.out.println("test 'aborttest' skipped  due to missing policy");
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

}
