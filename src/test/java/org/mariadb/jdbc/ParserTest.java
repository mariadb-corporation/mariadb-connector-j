package org.mariadb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mariadb.jdbc.internal.common.query.IllegalParameterException;

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

	@Test()
	public void testJDBCParserSimpleIPv4basic() {
		String url = "jdbc:mysql://master:3306,slave1:3307,slave2:3308/database";
		JDBCUrl jdbcUrl = JDBCUrl.parse(url);
	}
	@Test
	public void testJDBCParserSimpleIPv4basicError() {
		try {
			JDBCUrl.parse(null);
			Assert.fail();
		}catch (IllegalArgumentException e) {
			Assert.assertTrue(true);
		}
	}
	@Test
	public void testJDBCParserSimpleIPv4basicwithoutDatabase() {
		String url = "jdbc:mysql://master:3306,slave1:3307,slave2:3308/";
		JDBCUrl jdbcUrl = JDBCUrl.parse(url);
		Assert.assertNull(jdbcUrl.getDatabase());
		Assert.assertNull(jdbcUrl.getUsername());
		Assert.assertNull(jdbcUrl.getPassword());
		Assert.assertTrue(jdbcUrl.getHostAddresses().length == 3);
		Assert.assertTrue(new HostAddress("master", 3306).equals(jdbcUrl.getHostAddresses()[0]));
		Assert.assertTrue(new HostAddress("slave1", 3307).equals(jdbcUrl.getHostAddresses()[1]));
		Assert.assertTrue(new HostAddress("slave2", 3308).equals(jdbcUrl.getHostAddresses()[2]));
	}

	@Test
	public void testJDBCParserSimpleIPv4Properties() {
		String url = "jdbc:mysql://master:3306,slave1:3307,slave2:3308/database?autoreconnect=true";
		Properties prop = new Properties();
		prop.setProperty("user","greg");
		prop.setProperty("password","pass");

		JDBCUrl jdbcUrl = JDBCUrl.parse(url, prop);
		Assert.assertTrue("database".equals(jdbcUrl.getDatabase()));
		Assert.assertTrue("greg".equals(jdbcUrl.getUsername()));
		Assert.assertTrue("pass".equals(jdbcUrl.getPassword()));
		Assert.assertTrue("true".equals(jdbcUrl.getProperties().getProperty("autoreconnect")));
		Assert.assertTrue(jdbcUrl.getHostAddresses().length == 3);
		Assert.assertTrue(new HostAddress("master", 3306).equals(jdbcUrl.getHostAddresses()[0]));
		Assert.assertTrue(new HostAddress("slave1", 3307).equals(jdbcUrl.getHostAddresses()[1]));
		Assert.assertTrue(new HostAddress("slave2", 3308).equals(jdbcUrl.getHostAddresses()[2]));
	}

	@Test
	public void testJDBCParserSimpleIPv4() {
		String url = "jdbc:mysql://master:3306,slave1:3307,slave2:3308/database?user=greg&password=pass";
		JDBCUrl jdbcUrl = JDBCUrl.parse(url);
		Assert.assertTrue("database".equals(jdbcUrl.getDatabase()));
		Assert.assertTrue("greg".equals(jdbcUrl.getUsername()));
		Assert.assertTrue("pass".equals(jdbcUrl.getPassword()));
		Assert.assertTrue(jdbcUrl.getHostAddresses().length == 3);
		Assert.assertTrue(new HostAddress("master", 3306).equals(jdbcUrl.getHostAddresses()[0]));
		Assert.assertTrue(new HostAddress("slave1", 3307).equals(jdbcUrl.getHostAddresses()[1]));
		Assert.assertTrue(new HostAddress("slave2", 3308).equals(jdbcUrl.getHostAddresses()[2]));
	}


	@Test
	public void testJDBCParserSimpleIPv6() {
		String url = "jdbc:mysql://[2001:0660:7401:0200:0000:0000:0edf:bdd7]:3306,[2001:660:7401:200::edf:bdd7]:3307/database?user=greg&password=pass";
		JDBCUrl jdbcUrl = JDBCUrl.parse(url);
		Assert.assertTrue("database".equals(jdbcUrl.getDatabase()));
		Assert.assertTrue("greg".equals(jdbcUrl.getUsername()));
		Assert.assertTrue("pass".equals(jdbcUrl.getPassword()));
		Assert.assertTrue(jdbcUrl.getHostAddresses().length == 2);
		Assert.assertTrue(new HostAddress("2001:0660:7401:0200:0000:0000:0edf:bdd7", 3306).equals(jdbcUrl.getHostAddresses()[0]));
		Assert.assertTrue(new HostAddress("2001:660:7401:200::edf:bdd7", 3307).equals(jdbcUrl.getHostAddresses()[1]));
	}


	@Test
	public void testJDBCParserParameter() {
		String url = "jdbc:mysql://address=(type=master)(port=3306)(host=master1),address=(port=3307)(type=master)(host=master2),address=(type=slave)(host=slave1)(port=3308)/database?user=greg&password=pass";
		JDBCUrl jdbcUrl = JDBCUrl.parse(url);
		Assert.assertTrue("database".equals(jdbcUrl.getDatabase()));
		Assert.assertTrue("greg".equals(jdbcUrl.getUsername()));
		Assert.assertTrue("pass".equals(jdbcUrl.getPassword()));
		Assert.assertTrue(jdbcUrl.getHostAddresses().length == 3);
		Assert.assertTrue(new HostAddress("master1", 3306, "master").equals(jdbcUrl.getHostAddresses()[0]));
		Assert.assertTrue(new HostAddress("master2", 3307, "master").equals(jdbcUrl.getHostAddresses()[1]));
		Assert.assertTrue(new HostAddress("slave1", 3308, "slave").equals(jdbcUrl.getHostAddresses()[2]));
	}

	@Test
	public void testJDBCParserParameterError() {
		String url = "jdbc:mysql://address=(type)(port=3306)(host=master1),address=(port=3307)(type=master)(host=master2),address=(type=slave)(host=slave1)(port=3308)/database?user=greg&password=pass";
		try {
			JDBCUrl.parse(null);
			Assert.fail();
		}catch (IllegalArgumentException e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void testJDBCParserParameterErrorEqual() {
		String url = "jdbc:mysql://address=(type=)(port=3306)(host=master1),address=(port=3307)(type=master)(host=master2),address=(type=slave)(host=slave1)(port=3308)/database?user=greg&password=pass";
		try {
			JDBCUrl.parse(null);
			Assert.fail();
		}catch (IllegalArgumentException e) {
			Assert.assertTrue(true);
		}
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
