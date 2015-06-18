package org.mariadb.jdbc;


import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;


public class BooleanTest extends BaseTest {
    @Test
    public void testBoolean() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("drop table  if exists booleantest");
        stmt.execute("create table booleantest (id int not null primary key auto_increment, test boolean)");
        stmt.execute("insert into booleantest values(null, true)");
        stmt.execute("insert into booleantest values(null, false)");
        ResultSet rs = stmt.executeQuery("select * from booleantest");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(2));
        assertTrue(rs.next());
        assertFalse(rs.getBoolean(2));
    }

}
