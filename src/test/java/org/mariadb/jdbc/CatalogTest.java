package org.mariadb.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class CatalogTest extends BaseTest {


    @Test
    public void catalogTest() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.executeUpdate("drop database if exists cattest1");
        stmt.executeUpdate("create database cattest1");
        sharedConnection.setCatalog("cattest1");
        assertEquals("cattest1", sharedConnection.getCatalog());
        stmt.executeUpdate("drop database if exists cattest1");
        sharedConnection.setCatalog(database);
    }

    @Test(expected = SQLException.class)
    public void catalogTest2() throws SQLException {
        sharedConnection.setCatalog(null);
    }

    @Test(expected = SQLException.class)
    public void catalogTest3() throws SQLException {
        sharedConnection.setCatalog("Non-existent catalog");
    }

    @Test(expected = SQLException.class)
    public void catalogTest4() throws SQLException {
        sharedConnection.setCatalog("");
    }

    @Test
    public void catalogTest5() throws SQLException {
        requireMinimumVersion(5, 1);


        String[] weirdDbNames = new String[]{"abc 123", "\"", "`"};
        for (String name : weirdDbNames) {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("drop database if exists " + MySQLConnection.quoteIdentifier(name));
            stmt.execute("create database " + MySQLConnection.quoteIdentifier(name));
            sharedConnection.setCatalog(name);
            assertEquals(name, sharedConnection.getCatalog());
            stmt.execute("drop database if exists " + MySQLConnection.quoteIdentifier(name));
            stmt.close();
            sharedConnection.setCatalog(database);
        }
    }
}
