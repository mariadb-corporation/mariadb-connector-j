package org.mariadb.jdbc;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
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
		SQLPermission sqlPermission = new SQLPermission("abort");
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
	 * CONJ-116: Make SQLException prettier when too large packet is sent to the server
	 * @throws SQLException
	 * @throws UnsupportedEncodingException 
	 */
	@Test
	public void maxAllowedPackedExceptionIsPrettyTest() throws SQLException, UnsupportedEncodingException {
		int maxAllowedPacket = 1024 * 1024;
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery("SHOW VARIABLES LIKE 'max_allowed_packet'");
		if (rs.next()) {
			maxAllowedPacket = rs.getInt(2);
		}
		rs.close();
		statement.execute("DROP TABLE IF EXISTS dummy");
		statement.execute("CREATE TABLE dummy (a BLOB)");
		//Create a SQL packet bigger than maxAllowedPacket
		StringBuilder sb = new StringBuilder();
		String rowData = "('this is a dummy row values')";
		int rowsToWrite = (maxAllowedPacket / rowData.getBytes("UTF-8").length) + 1;
		for (int row = 1;  row <= rowsToWrite; row++) {
			if (row >= 2) {
				sb.append(", ");
			}
			sb.append(rowData);
		}
		String sql = "INSERT INTO dummy VALUES " + sb.toString();
		try {
			statement.executeUpdate(sql);
			fail("The previous statement should throw an SQLException");
		} catch (SQLException e) {
			assertTrue(e.getMessage().contains("max_allowed_packet"));
		} catch (Exception e) {
			fail("The previous statement should throw an SQLException not a general Exception");
		} finally {
			statement.execute("DROP TABLE dummy");
		}
	}

}
