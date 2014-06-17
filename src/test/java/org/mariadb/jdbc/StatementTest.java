package org.mariadb.jdbc;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class StatementTest extends BaseTest {

	public StatementTest() {
	}
	
	@Test
	public void wrapperTest() throws SQLException {
		MySQLStatement mysqlStatement = new MySQLStatement((MySQLConnection) connection);
		assertTrue(mysqlStatement.isWrapperFor(Statement.class));
		assertFalse(mysqlStatement.isWrapperFor(SQLException.class));
		assertThat(mysqlStatement.unwrap(Statement.class), equalTo((Statement)mysqlStatement));
		try {
			mysqlStatement.unwrap(SQLException.class);
			fail("MySQLStatement class unwrapped as SQLException class");
		} catch (SQLException sqle) {
			assertTrue(true);
		} catch (Exception e) {
			assertTrue(false);
		}
		mysqlStatement.close();
	}
	
	@Test(expected=SQLException.class)
	public void afterConnectionClosedTest() throws SQLException {
		Connection conn2 = DriverManager.getConnection("jdbc:mariadb://localhost:3306/test?user=root");
		Statement st1 = conn2.createStatement();
		conn2.close();
		Statement st2 = conn2.createStatement();
	}

}