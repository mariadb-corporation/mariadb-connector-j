package org.mariadb.jdbc;

import org.junit.Test;

import java.sql.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import static junit.framework.Assert.assertEquals;


public class UnicodeTest extends BaseTest {
static { Logger.getLogger("").setLevel(Level.OFF); }

    @Test
    public void firstTest() throws SQLException {
        String jaString = "\u65e5\u672c\u8a9e\u6587\u5b57\u5217"; // hmm wonder what this means...
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop table if exists unicode_test");
        stmt.executeUpdate("create table unicode_test (id int not null primary key auto_increment, test_text varchar(100)) charset utf8");
        PreparedStatement ps = connection.prepareStatement("insert into unicode_test (test_text) values (?)");
        ps.setString(1,jaString);
        ps.executeUpdate();
        ResultSet rs = stmt.executeQuery("select test_text from unicode_test");
        assertEquals(true,rs.next());
        assertEquals(jaString,rs.getString(1));
    }

    @Test
    public void testGermanUmlauts() throws SQLException {
        String query = "insert into umlaut_test values('tax-1273608028038--5546415852995205209-13', 'MwSt. 7% Bücher & Lebensmittel', 7)";
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop table if exists umlaut_test");
        stmt.executeUpdate("create table umlaut_test (id varchar(100), test_text varchar(100), t int) charset utf8");
        stmt.executeUpdate(query);

        ResultSet rs = stmt.executeQuery("select * from umlaut_test");
        assertEquals(true, rs.next());
        assertEquals("MwSt. 7% Bücher & Lebensmittel", rs.getString(2));
        assertEquals(false, rs.next());
    }
    @Test
    public void mysqlTest() throws SQLException {
        String jaString = "\u65e5\u672c\u8a9e\u6587\u5b57\u5217"; // hmm wonder what this means...
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop table if exists unicode_test2");
        stmt.executeUpdate("create table unicode_test2 (id int not null primary key auto_increment, test_text varchar(100)) charset=utf8");
        PreparedStatement ps = connection.prepareStatement("insert into unicode_test2 (test_text) values (?)");
        ps.setString(1,jaString);
        ps.executeUpdate();
        ResultSet rs = stmt.executeQuery("select test_text from unicode_test2");
        assertEquals(true,rs.next());
        assertEquals(jaString,rs.getString(1));
    }
}