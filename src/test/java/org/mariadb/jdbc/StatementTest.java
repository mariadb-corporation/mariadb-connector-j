package org.mariadb.jdbc;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

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

}