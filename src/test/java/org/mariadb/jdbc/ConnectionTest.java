package org.mariadb.jdbc;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
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
		assertTrue(properties.getProperty("user").equalsIgnoreCase(username));
		// assertTrue(properties.getProperty("password").equalsIgnoreCase(password));
		assertTrue(properties.getProperty("useFractionalSeconds").equalsIgnoreCase("true"));
		assertTrue(properties.getProperty("allowMultiQueries").equalsIgnoreCase("true"));
		assertTrue(properties.getProperty("useCompression").equalsIgnoreCase("false"));
		assertEquals(username, connection.getClientInfo("user"));
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
		} catch (SQLException sqlex) {
			assertTrue(sqlex.getMessage().contains("negative"));
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
                try {
			for (int row = 1;  row <= rowsToWrite; row++) {
				if (row >= 2) {
					sb.append(", ");
				}
				sb.append(rowData);
			}
		} catch (OutOfMemoryError e) {
			System.out.println("skip test 'maxAllowedPackedExceptionIsPrettyTest' - not enough memory");
			return;
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
