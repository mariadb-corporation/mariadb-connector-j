package org.mariadb.jdbc;

import static org.junit.Assert.*;

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

}
