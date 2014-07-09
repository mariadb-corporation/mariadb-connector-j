package org.mariadb.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
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
		String params = "user=root&useFractionalSeconds=true&allowMultiQueries=true&useCompression=false";
		Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?" + params);
		Properties properties = conn.getClientInfo();
		assertTrue(properties != null);
		assertTrue(properties.size() > 0);
		assertTrue(properties.getProperty("user") == null);
		assertTrue(properties.getProperty("password") == null);
		assertTrue(properties.getProperty("useFractionalSeconds").equalsIgnoreCase("true"));
		assertTrue(properties.getProperty("allowMultiQueries").equalsIgnoreCase("true"));
		assertTrue(properties.getProperty("useCompression").equalsIgnoreCase("false"));
		assertEquals(null, conn.getClientInfo("user"));
		assertEquals(null, conn.getClientInfo("password"));
		assertEquals("true", conn.getClientInfo("useFractionalSeconds"));
		assertEquals("true", conn.getClientInfo("allowMultiQueries"));
		assertEquals("false", conn.getClientInfo("useCompression"));
	}
	
	/**
	 * CONJ-75
	 * Needs permission java.sql.SQLPermission "abort" or will be skipped
	 * @throws SQLException
	 */
	@Test
	public void abortTest() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root");
		Statement stmt = conn.createStatement();
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
		conn.abort(executor);
		assertTrue(conn.isClosed());
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
