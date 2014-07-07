package org.mariadb.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

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
	}

}
