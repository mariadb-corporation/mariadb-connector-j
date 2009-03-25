package org.drizzle.jdbc;

import org.junit.Test;
import org.apache.log4j.BasicConfigurator;

import java.sql.*;

import static junit.framework.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 1, 2009
 * Time: 4:56:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class UnicodeTest {
       static { BasicConfigurator.configure(); }
    @Test
    public void firstTest() throws SQLException {
        try {
            Class.forName("org.drizzle.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("Could not load driver");
        }
        Connection connection = DriverManager.getConnection("jdbc:drizzle://"+DriverTest.host+":4427/test_units_jdbc");
        String jaString = "\u65e5\u672c\u8a9e\u6587\u5b57\u5217"; // hmm wonder what this means...
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop table if exists unicode_test");
        stmt.executeUpdate("create table unicode_test (test_text varchar(100))");
        PreparedStatement ps = connection.prepareStatement("insert into unicode_test values (?)");
        ps.setString(1,jaString);
        ps.executeUpdate();
        ResultSet rs = stmt.executeQuery("select * from unicode_test");
        assertEquals(true,rs.next());
        assertEquals(jaString,rs.getString(1));
    }
}