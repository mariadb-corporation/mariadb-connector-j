package org.mariadb.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;


public class UnicodeTest extends BaseTest {

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("unicode_test", "id int not null primary key auto_increment, test_text varchar(100)", "charset utf8");
        createTable("umlaut_test", "id varchar(100), test_text varchar(100), t int", "charset utf8");
        createTable("unicode_test2", "id int not null primary key auto_increment, test_text varchar(100)", "charset=utf8");
    }

    @Test
    public void firstTest() throws SQLException {
        String jaString = "\u65e5\u672c\u8a9e\u6587\u5b57\u5217"; // hmm wonder what this means...
        Statement stmt = sharedConnection.createStatement();
        PreparedStatement ps = sharedConnection.prepareStatement("insert into unicode_test (test_text) values (?)");
        ps.setString(1, jaString);
        ps.executeUpdate();
        ResultSet rs = stmt.executeQuery("select test_text from unicode_test");
        assertEquals(true, rs.next());
        assertEquals(jaString, rs.getString(1));
    }

    @Test
    public void testGermanUmlauts() throws SQLException {
        String query = "insert into umlaut_test values('tax-1273608028038--5546415852995205209-13', 'MwSt. 7% Bücher & Lebensmittel', 7)";
        Statement stmt = sharedConnection.createStatement();
        stmt.executeUpdate(query);

        ResultSet rs = stmt.executeQuery("select * from umlaut_test");
        assertEquals(true, rs.next());
        assertEquals("MwSt. 7% Bücher & Lebensmittel", rs.getString(2));
        assertEquals(false, rs.next());
    }

    @Test
    public void mysqlTest() throws SQLException {
        String jaString = "\u65e5\u672c\u8a9e\u6587\u5b57\u5217"; // hmm wonder what this means...
        Statement stmt = sharedConnection.createStatement();
        PreparedStatement ps = sharedConnection.prepareStatement("insert into unicode_test2 (test_text) values (?)");
        ps.setString(1, jaString);
        ps.executeUpdate();
        ResultSet rs = stmt.executeQuery("select test_text from unicode_test2");
        assertEquals(true, rs.next());
        assertEquals(jaString, rs.getString(1));
    }
}