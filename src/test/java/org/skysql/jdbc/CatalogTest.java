package org.skysql.jdbc;

import org.junit.Test;
import org.skysql.jdbc.internal.common.Utils;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static junit.framework.Assert.assertEquals;

public class CatalogTest extends BaseTest {
    static { Logger.getLogger("").setLevel(Level.FINEST); }

    public CatalogTest() {
    }

    @Test
    public void catalogTest() throws SQLException {
    	Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop database if exists cattest1");
        stmt.executeUpdate("create database cattest1");
        connection.setCatalog("cattest1");
        assertEquals("cattest1", connection.getCatalog());
    }
    
    @Test(expected = SQLException.class)
    public void catalogTest2() throws SQLException {
        connection.setCatalog(null);
    }
    
    @Test(expected = SQLException.class)
    public void catalogTest3() throws SQLException {
        connection.setCatalog("Non-existant catalog");
    }  
    
    @Test(expected = SQLException.class)
    public void catalogTest4() throws SQLException {
        connection.setCatalog("");
    } 
    
    @Test(expected = SQLException.class)
    public void catalogTest5() throws SQLException {
    	Statement stmt = connection.createStatement();
        connection.setCatalog("\"quoted\"");
    }
}
