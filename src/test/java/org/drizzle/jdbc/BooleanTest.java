package org.drizzle.jdbc;


import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class BooleanTest {
    @Test
    public void testBoolean() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:drizzle://root@"+DriverTest.host+":4427/test_units_jdbc");
        Statement stmt = conn.createStatement();
        stmt.execute("drop table  if exists booleantest");
        stmt.execute("create table booleantest (id int not null primary key auto_increment, test boolean)");
        stmt.execute("insert into booleantest values(null, true)");
        stmt.execute("insert into booleantest values(null, false)");
        ResultSet rs = stmt.executeQuery("select * from booleantest");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(2));
        assertEquals(Boolean.class, rs.getObject(2).getClass());
        assertTrue(rs.next());
        assertFalse(rs.getBoolean(2));
        assertEquals(Boolean.class, rs.getObject(2).getClass());

    }

}
