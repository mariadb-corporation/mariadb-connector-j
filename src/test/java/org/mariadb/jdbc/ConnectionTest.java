package org.mariadb.jdbc;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLPermission;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.junit.Test;
import org.mariadb.jdbc.internal.mysql.MySQLProtocol;

public class ConnectionTest extends BaseTest {

	/**
	 * CONJ-89
	 * 
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
		// assertTrue(properties.getProperty("password").equalsIgnoreCase(mPassword));
		assertTrue(properties.getProperty("useFractionalSeconds").equalsIgnoreCase("true"));
		assertTrue(properties.getProperty("allowMultiQueries").equalsIgnoreCase("true"));
		assertTrue(properties.getProperty("useCompression").equalsIgnoreCase("false"));
		assertEquals(mUsername, connection.getClientInfo("user"));
		// assertEquals(null, connection.getClientInfo("password"));
		assertEquals("true", connection.getClientInfo("useFractionalSeconds"));
		assertEquals("true", connection.getClientInfo("allowMultiQueries"));
		assertEquals("false", connection.getClientInfo("useCompression"));
	}

	/**
	 * CONJ-75
	 * Needs permission java.sql.SQLPermission "abort" or will be skipped
	 * 
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

	/**
	 * CONJ-120 Fix Connection.isValid method
	 * 
	 * @throws SQLException
	 */
	@Test
	public void isValid_shouldThrowExceptionWithNegativeTimeout()
			throws SQLException {
		try {
			connection.isValid(-1);
			fail("The above row should have thrown an SQLException");
		} catch (SQLException e) {
			assertTrue(e.getMessage().contains("negative"));
		}
	}

	@Test
	public void isValid_testWorkingConnection() throws SQLException {
		assertTrue(connection.isValid(0));
	}

	/**
	 * CONJ-120 Fix Connection.isValid method
	 * 
	 * @throws SQLException
	 */
	@Test
	public void isValid_closedConnection() throws SQLException {
		connection.close();
		boolean isValid = connection.isValid(0);
		assertFalse(isValid);
	}

	/**
	 * CONJ-120 Fix Connection.isValid method
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	@Test
	public void isValid_connectionThatTimesOutByServer() throws SQLException,
			InterruptedException {
		Statement statement = connection.createStatement();
		statement.execute("set session wait_timeout=1");
		Thread.sleep(3000); // Wait for the server to kill the connection
		boolean isValid = connection.isValid(0);
		assertFalse(isValid);
		statement.close();
	}

	/**
	 * CONJ-120 Fix Connection.isValid method
	 * 
	 * @throws Exception
	 */
	@Test
	public void isValid_connectionThatIsKilledExternally() throws Exception {
		long threadId = getServerThreadId();
		Connection killerConnection = openNewConnection();
		Statement killerStatement = killerConnection.createStatement();
		killerStatement.execute("KILL CONNECTION " + threadId);
		killerConnection.close();
		boolean isValid = connection.isValid(0);
		assertFalse(isValid);
	}

	/**
	 * Reflection magic to extract the connection thread id assigned by the
	 * server
	 */
	private long getServerThreadId() throws Exception {
		Field protocolField = org.mariadb.jdbc.MySQLConnection.class.getDeclaredField("protocol");
		protocolField.setAccessible(true);
		MySQLProtocol protocol = (MySQLProtocol) protocolField.get(connection);
		Field serverThreadIdField = MySQLProtocol.class.getDeclaredField("serverThreadId");
		serverThreadIdField.setAccessible(true);
		long threadId = serverThreadIdField.getLong(protocol);
		return threadId;
	}

}
