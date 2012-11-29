package org.mariadb.jdbc;

import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

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
}
