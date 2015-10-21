package org.mariadb.jdbc;


import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class BooleanTest extends BaseTest {
    /**
     * Initialization.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("booleantest", "id int not null primary key auto_increment, test boolean");
    }

    @Test
    public void testBoolean() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("insert into booleantest values(null, true)");
        stmt.execute("insert into booleantest values(null, false)");
        ResultSet rs = stmt.executeQuery("select * from booleantest");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(2));
        assertTrue(rs.next());
        assertFalse(rs.getBoolean(2));
    }

}
