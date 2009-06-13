package org.drizzle.jdbc;

import org.junit.After;
import org.junit.Test;

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
public class DriverTestMySQL extends DriverTest {
    private Connection connection;
    public DriverTestMySQL() throws SQLException {
        connection = DriverManager.getConnection("jdbc:mysql:thin://localhost:3306/test_units_jdbc");
    }
    @Override
    public Connection getConnection() {
        return connection;
    }
    @Test
    public void a() {
    }
}
