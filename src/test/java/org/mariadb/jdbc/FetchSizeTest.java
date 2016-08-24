package org.mariadb.jdbc;

import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class FetchSizeTest extends BaseTest {

    /**
     * Tables initialisation.
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("fetchSizeTest1", "id int, test varchar(100)");
        createTable("fetchSizeTest2", "id int, test varchar(100)");
        createTable("fetchSizeTest3", "id int, test varchar(100)");
        createTable("fetchSizeTest4", "id int, test varchar(100)");
    }

    @Test
    public void batchFetchSizeTest() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        PreparedStatement pstmt = sharedConnection.prepareStatement("INSERT INTO fetchSizeTest1 (test) values (?)");
        stmt.setFetchSize(1);
        pstmt.setFetchSize(1);
        //check that fetch isn't use by batch execution
        for (int i = 0; i < 10; i++) {
            pstmt.setString(1, "" + i);
            pstmt.addBatch();
            stmt.addBatch("INSERT INTO fetchSizeTest1 (test) values ('aaa" + i + "')");
        }
        pstmt.executeBatch();
        stmt.executeBatch();

        ResultSet resultSet = stmt.executeQuery("SELECT count(*) from fetchSizeTest1");
        if (resultSet.next()) {
            assertEquals(20, resultSet.getLong(1));
        } else {
            fail("must have resultset");
        }

    }

    @Test
    public void fetchSizeNormalTest() throws SQLException {
        prepare100record("fetchSizeTest4");

        Statement stmt = sharedConnection.createStatement();
        stmt.setFetchSize(1);
        ResultSet resultSet = stmt.executeQuery("SELECT test FROM fetchSizeTest4");
        for (int counter = 0; counter < 100; counter++) {
            assertTrue(resultSet.next());
            assertEquals("" + counter, resultSet.getString(1));
        }
        assertFalse(resultSet.next());
    }


    @Test
    public void fetchSizeErrorWhileFetchTest() throws SQLException {
        prepare100record("fetchSizeTest3");

        Statement stmt = sharedConnection.createStatement();
        stmt.setFetchSize(1);
        ResultSet resultSet = stmt.executeQuery("SELECT test FROM fetchSizeTest3");
        for (int counter = 0; counter < 50; counter++) {
            assertTrue(resultSet.next());
            assertEquals("" + counter, resultSet.getString(1));
        }
        assertFalse(resultSet.isClosed());

        try {
            ResultSet rs2 = stmt.executeQuery("SELECT 1");
            if (rs2.next()) {
                assertEquals(1, rs2.getInt(1));
            } else {
                fail("resultset must have been active");
            }
        } catch (SQLException e) {
            fail("Must have worked");
        }

        try {
            assertFalse(resultSet.isClosed());
            for (int counter = 50; counter < 100; counter++) {
                assertTrue(resultSet.next());
                assertEquals("" + counter, resultSet.getString(1));
            }
            resultSet.close();
            assertTrue(resultSet.isClosed());
        } catch (SQLException sqlexception) {
            fail("must have throw an exception, since resulset must have been closed.");
        }
    }


    private void prepare100record(String tableName) throws SQLException {
        PreparedStatement pstmt = sharedConnection.prepareStatement("INSERT INTO " + tableName + " (test) values (?)");
        for (int i = 0; i < 100; i++) {
            pstmt.setString(1, "" + i);
            pstmt.addBatch();
        }
        pstmt.executeBatch();
    }
}
