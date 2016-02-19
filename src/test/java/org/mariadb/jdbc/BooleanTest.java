package org.mariadb.jdbc;


import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

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
        createTable("booleanAllField", "t1 BIT, t2 TINYINT(1), t3 SMALLINT(1), t4 MEDIUMINT(1), t5 INT(1), t6 BIGINT(1), t7 DECIMAL(1), t8 FLOAT, "
                + "t9 DOUBLE, t10 CHAR(1), t11 VARCHAR(1), t12 BINARY(1), t13 BLOB(1), t14 TEXT(1)");
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
                    assertTrue(rs.getBoolean(1));
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


    /**
     * CONJ-254 error when using scala anorm string interpolation.
     *
     * @throws SQLException exception
     */
    @Test
    public void testBooleanAllField() throws Exception {
        try (Connection connection = setConnection("&maxPerformance=true")) {
            Statement stmt = connection.createStatement();
            stmt.execute("INSERT INTO booleanAllField VALUES (null, null, null, null, null, null, null, null, null, null, null, null, null, null)");
            stmt.execute("INSERT INTO booleanAllField VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, '0', '0', '0', '0', '0')");
            stmt.execute("INSERT INTO booleanAllField VALUES (1, 1, 1, 1, 1, 1, 1, 1, 1, '1', '1', '1', '1', '1')");
            stmt.execute("INSERT INTO booleanAllField VALUES (1, 2, 2, 2, 2, 2, 2, 2, 2, '2', '2', '2', '2', '2')");

            ResultSet rs = stmt.executeQuery("SELECT * FROM booleanAllField");
            checkBooleanValue(rs, false, null);
            checkBooleanValue(rs, false, false);
            checkBooleanValue(rs, true, true);
            checkBooleanValue(rs, true, true);

            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM booleanAllField WHERE 1 = ?");
            preparedStatement.setInt(1, 1);
            rs = preparedStatement.executeQuery();
            checkBooleanValue(rs, false, null);
            checkBooleanValue(rs, false, false);
            checkBooleanValue(rs, true, true);
            checkBooleanValue(rs, true, true);
        }
    }

    private void checkBooleanValue(ResultSet rs, boolean expectedValue, Boolean expectedNull) throws SQLException {
        rs.next();
        for (int i = 1; i <= 14; i++ ) {
            assertEquals(expectedValue, rs.getBoolean(i));
            if (i == 1 || i == 2) {
                assertEquals(expectedNull, rs.getObject(i));
            }
        }
    }
}
