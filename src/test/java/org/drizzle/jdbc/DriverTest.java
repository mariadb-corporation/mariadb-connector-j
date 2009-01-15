package org.drizzle.jdbc;

import org.junit.Test;
import org.junit.Before;

import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.Connection;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 7:58:11 AM
 */
public class DriverTest {
    @Before
    public void setup()
    {
        try {
            Class.forName("org.drizzle.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
    @Test
    public void connect() throws SQLException {
        Connection connection = DriverManager.getConnection("localhost","","");
    }
}
