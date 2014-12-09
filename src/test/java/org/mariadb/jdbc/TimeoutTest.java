package org.mariadb.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class TimeoutTest extends BaseTest {

	/**
	 * CONJ-79
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
	 * @throws SQLException
	 */
	@Test
	public void socketTimeoutTest() throws SQLException {
		setConnection("&connectTimeout=5&socketTimeout=5");
		PreparedStatement ps = connection.prepareStatement("SELECT sleep(1)");
		try {
			ps.executeQuery();
		} catch (Exception e) {
		} finally {
			assertTrue(connection.isClosed());
		}
	}

}