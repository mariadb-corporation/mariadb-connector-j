package org.drizzle.jdbc;

import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Connection;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jun 13, 2009
 * Time: 1:29:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class MySQLDriverTest extends DriverTest {
    private Connection connection;
    public MySQLDriverTest() throws SQLException {
        connection = DriverManager.getConnection("jdbc:mysql:thin://localhost:3306/test_units_jdbc");
    }
    @Override
    public Connection getConnection() {
        return connection;
    }
    
    @Test
    public void testAuthConnection() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:mysql:thin://test:test@localhost:3306/test_units_jdbc");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from t1");
        rs.close();
        stmt.close();
        conn.close();
    }
}
