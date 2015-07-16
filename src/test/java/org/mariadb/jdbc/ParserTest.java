package org.mariadb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mariadb.jdbc.internal.common.Options;
import org.mariadb.jdbc.internal.common.UrlHAMode;
import org.mariadb.jdbc.internal.common.query.IllegalParameterException;
import org.mariadb.jdbc.internal.mysql.Protocol;

public class ParserTest extends BaseTest {
	private Statement statement;

	@Before
	public void setup() throws SQLException {
		statement = connection.createStatement();
	}

	@After
	public void cleanup() {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
			}
		}
	}

	@Test
	public void addProperties() throws  Exception {
		Field field = MySQLConnection.class.getDeclaredField("options");
		field.setAccessible(true);
		Options options = (Options) field.get(connection);
		assertFalse(options.useSSL);
		connection.setClientInfo("useSSL", "true");

		options = (Options) field.get(connection);
		assertTrue(options.useSSL);

		Properties prop = new Properties();
		prop.put("autoReconnect", "true");
		prop.put("useSSL", "false");
		connection.setClientInfo(prop);
		assertFalse(options.useSSL);
		assertTrue(options.autoReconnect);
	}

	@Test
	public void libreOfficeBase() {
		String sql;
		try {
			sql = "DROP TABLE IF EXISTS table1";
			statement.execute(sql);
			sql = "DROP TABLE IF EXISTS table2";
			statement.execute(sql);
			sql = "CREATE TABLE table1 (id1 int auto_increment primary key)";
			statement.execute(sql);
			sql = "INSERT INTO table1 VALUES (1),(2),(3),(4),(5),(6)";
			statement.execute(sql);
			sql = "CREATE TABLE table2 (id2 int auto_increment primary key)";
			statement.execute(sql);
			sql = "INSERT INTO table2 VALUES (1),(2),(3),(4),(5),(6)";
			statement.execute(sql);
			// uppercase OJ
			sql = "SELECT table1.id1, table2.id2 FROM { OJ table1 LEFT OUTER JOIN table2 ON table1.id1 = table2.id2 }";
			ResultSet rs = statement.executeQuery(sql);
			for (int count=1; count<=6; count++) {
				assertTrue(rs.next());
				assertEquals(count, rs.getInt(1));
				assertEquals(count, rs.getInt(2));
			}
			// mixed oJ
			sql = "SELECT table1.id1, table2.id2 FROM { oJ table1 LEFT OUTER JOIN table2 ON table1.id1 = table2.id2 }";
			rs = statement.executeQuery(sql);
			for (int count=1; count<=6; count++) {
				assertTrue(rs.next());
				assertEquals(count, rs.getInt(1));
				assertEquals(count, rs.getInt(2));
			}
		} catch (SQLException e) {
			assertTrue(false);
		}
	}
}
