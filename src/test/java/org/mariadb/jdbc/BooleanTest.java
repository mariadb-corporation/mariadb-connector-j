package org.mariadb.jdbc;


import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;


public class BooleanTest extends BaseTest {
    /**
     * Initialization.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("booleantest", "id int not null primary key auto_increment, test boolean");
        createTable("booleanvalue", "test boolean");

    }

    @Test
    public void testBoolean() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("insert into booleantest values(null, true)");
        stmt.execute("insert into booleantest values(null, false)");
        ResultSet rs = stmt.executeQuery("select * from booleantest");
        if (rs.next()) {
            assertTrue(rs.getBoolean(2));
            if (rs.next()) {
                assertFalse(rs.getBoolean(2));
            } else {
                fail("must have a result !");
            }
        } else {
            fail("must have a result !");
        }

    }

    @Test
    public void testBooleanString() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("insert into booleanvalue values(true)");
        stmt.execute("insert into booleanvalue values(false)");
        stmt.execute("insert into booleanvalue values(4)");
        ResultSet rs = stmt.executeQuery("select * from booleanvalue");

        if (rs.next()) {
            assertTrue(rs.getBoolean(1));
            assertEquals("1", rs.getString(1));
            if (rs.next()) {
                assertFalse(rs.getBoolean(1));
                assertEquals("0", rs.getString(1));
                if (rs.next()) {
                    assertFalse(rs.getBoolean(1));
                    assertEquals("4", rs.getString(1));
                } else {
                    fail("must have a result !");
                }
            } else {
                fail("must have a result !");
            }
        } else {
            fail("must have a result !");
        }
    }
}
