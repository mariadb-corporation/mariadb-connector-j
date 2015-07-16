package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

public class DataSourceTest extends BaseTest {
    protected static final String defConnectToIP = null;
    protected static String connectToIP;

    @BeforeClass
    public static void beforeClassDataSourceTest() {
        connectToIP = System.getProperty("testConnectToIP", defConnectToIP);
    }

    @Test
    public void testDataSource() throws SQLException {
    	MySQLDataSource ds = new MySQLDataSource(hostname, port, database);
    	Connection connection = ds.getConnection(username, password);
        try {
            assertEquals(connection.isValid(0),true);
        } finally  {
            connection.close();
        }
    }
    @Test
    public void testDataSource2() throws SQLException {
    	MySQLDataSource ds = new MySQLDataSource(hostname, port, database);
    	Connection connection = ds.getConnection(username, password);
        try {
            assertEquals(connection.isValid(0),true);
        }finally {
            connection.close();
        }
    }

    @Test
    public void testDataSourceEmpty() throws SQLException {
        MySQLDataSource ds = new MySQLDataSource();
        ds.setDatabaseName(database);
        ds.setPort(port);
        ds.setServerName(hostname);
        Connection connection = ds.getConnection(username, password);
        try {
            assertEquals(connection.isValid(0),true);
        }finally {
            connection.close();
        }
    }
    /**
     * CONJ-80
     * @throws SQLException
     */
    @Test
    public void setDatabaseNameTest() throws SQLException {
    	MySQLDataSource ds = new MySQLDataSource(hostname, port, database);
    	Connection connection = ds.getConnection(username, password);
    	connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test2");
    	ds.setDatabaseName("test2");
    	connection = ds.getConnection(username, password);
    	assertEquals("test2", ds.getDatabaseName());
    	assertEquals(ds.getDatabaseName(), connection.getCatalog());
    	connection.createStatement().execute("DROP DATABASE IF EXISTS test2");
    	connection.close();
    }

    /**
     * CONJ-80
     * @throws SQLException
     */
    @Test
    public void setServerNameTest() throws SQLException {
        Assume.assumeTrue(connectToIP != null);
    	MySQLDataSource ds = new MySQLDataSource(hostname, port, database);
    	Connection connection = ds.getConnection(username, password);
    	ds.setServerName(connectToIP);
    	connection = ds.getConnection(username, password);
    	connection.close();
    }

    /**
     * CONJ-80
     * @throws SQLException
     */
    @Test // unless port 3307 can be used
    public void setPortTest() throws SQLException {


    	MySQLDataSource ds = new MySQLDataSource(hostname, port, database);
    	Connection connection2 = ds.getConnection(username, password);
        //delete blacklist, because can failover on 3306 is filled
        assureBlackList(connection2);
        connection2.close();

        ds.setPort(3307);

        //must throw SQLException
        try {
            ds.getConnection(username, password);
            Assert.fail();
        } catch (SQLException e) {
            log.fine("port error : " +e.getMessage());
        }
    }

    /**
     * CONJ-123:
     * Session variables lost and exception if set via MySQLDataSource.setProperties/setURL
     * @throws SQLException
     */
    @Test
    public void setPropertiesTest() throws SQLException {
    	MySQLDataSource ds = new MySQLDataSource(hostname, port, database);
    	ds.setProperties("sessionVariables=sql_mode='PIPES_AS_CONCAT'");
    	Connection connection = ds.getConnection(username, password);
    	ResultSet rs = connection.createStatement().executeQuery("SELECT @@sql_mode");
    	assertTrue(rs.next());
    	assertEquals("PIPES_AS_CONCAT", rs.getString(1));
    	ds.setUrl(connURI + "&sessionVariables=sql_mode='ALLOW_INVALID_DATES'");
    	connection = ds.getConnection();
    	rs = connection.createStatement().executeQuery("SELECT @@sql_mode");
    	assertTrue(rs.next());
    	assertEquals("ALLOW_INVALID_DATES", rs.getString(1));
    	connection.close();
    }
}
