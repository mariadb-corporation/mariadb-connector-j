package org.mariadb.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.Test;

public class DataSourceTest {
    @Test
    public void testDataSource() throws SQLException {
        DataSource ds = new MySQLDataSource("localhost",3306,"test");
        Connection connection = ds.getConnection("root", null);
        try {
            assertEquals(connection.isValid(0),true);
        } finally  {
            connection.close();
        }
    }
    @Test
    public void testDataSource2() throws SQLException {
        DataSource ds = new MySQLDataSource("localhost",3306,"test");
        Connection connection = ds.getConnection("root","");
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
    	MySQLDataSource ds = new MySQLDataSource("localhost", 3306, "test");
    	Connection connection = ds.getConnection("root", "");
    	connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test2");
    	ds.setDatabaseName("test2");
    	connection = ds.getConnection("root", "");
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
    	MySQLDataSource ds = new MySQLDataSource("localhost", 3306, "test");
    	Connection connection = ds.getConnection("root", "");
    	ds.setServerName("127.0.0.1");
    	connection = ds.getConnection("root", "");
    	connection.close();
    }
    
    /**
     * CONJ-80
     * @throws SQLException
     */
    @Test(expected=SQLException.class) // unless port 3307 can be used
    public void setPortTest() throws SQLException {
    	MySQLDataSource ds = new MySQLDataSource("localhost", 3306, "test");
    	Connection connection = ds.getConnection("root", "");
    	ds.setPort(3307);
    	connection = ds.getConnection("root", "");
    	connection.close();
    }
}
