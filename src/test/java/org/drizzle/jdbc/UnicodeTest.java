package org.drizzle.jdbc;

import org.junit.Test;

import java.sql.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import static junit.framework.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 1, 2009
 * Time: 4:56:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class UnicodeTest {
static { Logger.getLogger("").setLevel(Level.OFF); }

    @Test
    public void firstTest() throws SQLException {

        Connection connection = DriverManager.getConnection("jdbc:drizzle://"+DriverTest.host+":4427/test_units_jdbc");
        String jaString = "\u65e5\u672c\u8a9e\u6587\u5b57\u5217"; // hmm wonder what this means...
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop table if exists unicode_test");
        stmt.executeUpdate("create table unicode_test (id int not null primary key auto_increment, test_text varchar(100))");
        PreparedStatement ps = connection.prepareStatement("insert into unicode_test (test_text) values (?)");
        ps.setString(1,jaString);
        ps.executeUpdate();
        ResultSet rs = stmt.executeQuery("select test_text from unicode_test");
        assertEquals(true,rs.next());
        assertEquals(jaString,rs.getString(1));
    }
}