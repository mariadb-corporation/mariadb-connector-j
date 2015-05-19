package org.mariadb.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;

public class TimeoutTest extends BaseTest {

	/**
	 * CONJ-79
	 * 
	 * @throws SQLException
	 */
	@Test
	public void resultSetAfterSocketTimeoutTest() throws SQLException {
		setConnection("&connectTimeout=5&socketTimeout=5");
		boolean bugReproduced = false;
		int exc = 0;
		int went = 0;
		for (int i = 0; i < 10000; i++) {
			try {
				int v1 = selectValue(connection, 1);
				int v2 = selectValue(connection, 2);
				if (v1 != 1 || v2 != 2) {
					bugReproduced = true;
					break;
				}
				assertTrue(v1 == 1 && v2 == 2);
				went++;
			} catch (Exception e) {
				exc++;
			}
		}
		assertFalse(bugReproduced); // either Exception or fine
		assertTrue(went > 0);
		assertTrue(went + exc == 10000);
	}

	private static int selectValue(Connection conn, int value)
			throws SQLException {
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select " + value);
			rs.next();
			return rs.getInt(1);
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (stmt != null) {
				stmt.close();
			}
		}
	}

	/**
	 * CONJ-79
	 * 
	 * @throws SQLException
	 */
	@Test
	public void socketTimeoutTest() throws SQLException {
		setConnection("&connectTimeout=5&socketTimeout=5");
		PreparedStatement ps = connection.prepareStatement("SELECT 1");
		ResultSet rs = ps.executeQuery();
		rs.next();
		logInfo(rs.getString(1));

		ps = connection.prepareStatement("SELECT sleep(1)");

		rs = ps.executeQuery();

		ps = connection.prepareStatement("SELECT 2");

		rs = ps.executeQuery();
		rs.next();
		logInfo(rs.getString(1));

		assertTrue(connection.isClosed());
	}

	@Test
	public void waitTimeoutStatementTest() throws SQLException, InterruptedException {
		Statement statement = connection.createStatement();
		statement.execute("set session wait_timeout=1");
		Thread.sleep(3000); // Wait for the server to kill the connection
		
		logInfo(connection.toString());
		
		statement.execute("SELECT 1");
		
		statement.close();
		connection.close();
		connection = null;
	}
	
	@Test
	public void waitTimeoutResultSetTest() throws SQLException, InterruptedException {
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT 1");
		
		rs.next();
		logInfo(rs.getString(1));

		//stmt = connection.createStatement();
		stmt.execute("set session wait_timeout=1");
		Thread.sleep(3000); // Wait for the server to kill the connection

		rs = stmt.executeQuery("SELECT 2");

		rs.next();
		logInfo(rs.getString(1));

		assertTrue(connection.isClosed());
	}
	
	// CONJ-68
	// TODO: this test is not able to repeat the bug. Ignore until then.
	@Ignore
	@Test
	public void lastPacketFailedTest() throws SQLException
	{
		Statement stmt = connection.createStatement();
		stmt.execute("DROP TABLE IF EXISTS `pages_txt`");
		stmt.execute("CREATE TABLE `pages_txt` (`id` INT(10) UNSIGNED NOT NULL, `title` TEXT NOT NULL, `txt` MEDIUMTEXT NOT NULL, PRIMARY KEY (`id`)) COLLATE='utf8_general_ci' ENGINE=MyISAM;");
	
		//create arbitrary long strings
		String chars = "0123456789abcdefghijklmnopqrstuvwxyzåäöABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ,;.:-_*¨^+?!<>#€%&/()=";
		StringBuffer outputBuffer = null;
		Random r = null;
		
		for(int i = 1; i < 2001; i++)
        {
			r = new Random();
			outputBuffer = new StringBuffer(i);
			
			for (int j = 0; j < i; j++){
				outputBuffer.append(chars.charAt(r.nextInt(chars.length())));
			}
			stmt.execute("insert into pages_txt values (" + i + ", '" + outputBuffer.toString() + "' , 'txt')");
        }
	}
}