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
    }

    @Test
    public void batchFetchSizeTest() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("TRUNCATE TABLE fetchSizeTest1");
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
        prepare1000record();

        Statement stmt = sharedConnection.createStatement();
        stmt.setFetchSize(1);
        ResultSet resultSet = stmt.executeQuery("SELECT test FROM fetchSizeTest1");
        for (int counter = 0; counter < 1000; counter++) {
            assertTrue(resultSet.next());
            assertEquals("" + counter, resultSet.getString(1));
        }
        assertFalse(resultSet.next());
    }

    @Test
    public void fetchSizeErrorWhileFetchTest() throws SQLException {
        prepare1000record();

        Statement stmt = sharedConnection.createStatement();
        stmt.setFetchSize(1);
        ResultSet resultSet = stmt.executeQuery("SELECT test FROM fetchSizeTest1");
        for (int counter = 0; counter < 500; counter++) {
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
            assertTrue(resultSet.isClosed());
            resultSet.next();
            fail("must have throw an exception, since resulset must have been closed.");
        } catch (SQLException sqlexception) {
            Assert.assertTrue(sqlexception.getMessage().contains("Operation not permit on a closed resultset"));
        }
    }


    @Test
    public void fetchSizeSpeedTest() throws SQLException {
        prepare1000record();

        Statement stmt = sharedConnection.createStatement();

        //test limit
        final long start = System.nanoTime();
        ResultSet resultSet = stmt.executeQuery("SELECT test FROM fetchSizeTest1 LIMIT 2");
        assertTrue(resultSet.next());
        assertEquals("0", resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals("1", resultSet.getString(1));
        long resultTimeLimit2 = System.nanoTime() - start;

        stmt.setFetchSize(0);
        //execute another query, so skipping result are not on next query
        stmt.executeQuery("SELECT 1");
        long resultTimeLimit2WithSkip = System.nanoTime() - start;
        System.out.println(resultTimeLimit2WithSkip + " / " + resultTimeLimit2);

        //test setMaxRows(2)
        final long start2 = System.nanoTime();
        stmt.setMaxRows(2);
        test2firstResult(stmt);
        long resultTimeFetchMaxRow = System.nanoTime() - start2;
        stmt.setMaxRows(0);
        stmt.executeQuery("SELECT 1");
        long resultTimeFetchMaxRowWithSkip = System.nanoTime() - start2;
        System.out.println(resultTimeFetchMaxRowWithSkip + " / " + resultTimeFetchMaxRow);

        //test fetch size 2
        stmt.setFetchSize(2);
        final long start3 = System.nanoTime();
        test2firstResult(stmt);
        long resultTimeFetch2 = System.nanoTime() - start3;
        stmt.setFetchSize(0);
        stmt.executeQuery("SELECT 1");
        long resultTimeFetch2WithSkip = System.nanoTime() - start3;
        System.out.println(resultTimeFetch2WithSkip + " / " + resultTimeFetch2);

        //test fetch all
        final long start4 = System.nanoTime();
        test2firstResult(stmt);
        long resultTimeFetchAll = System.nanoTime() - start4;
        stmt.executeQuery("SELECT 1");
        long resultTimeFetchAllWithSkip = System.nanoTime() - start4;
        System.out.println(resultTimeFetchAllWithSkip + " / " + resultTimeFetchAll);

        //normally this is right, but since server is caching rows, that may not be always the case.
//        assertTrue(resultTimeFetchMaxRowWithSkip > resultTimeLimit2WithSkip);
//        assertTrue(resultTimeFetch2WithSkip > resultTimeFetchMaxRowWithSkip);
//        assertTrue(resultTimeFetchAllWithSkip > resultTimeFetch2WithSkip);

    }

    private void test2firstResult(Statement statement) throws SQLException {
        ResultSet resultSet = statement.executeQuery("SELECT test FROM fetchSizeTest1");
        assertTrue(resultSet.next());
        assertEquals("0", resultSet.getString(1));
        assertTrue(resultSet.next());
        assertEquals("1", resultSet.getString(1));

    }

    private void prepare1000record() throws SQLException {
        sharedConnection.createStatement().execute("TRUNCATE TABLE fetchSizeTest1");
        PreparedStatement pstmt = sharedConnection.prepareStatement("INSERT INTO fetchSizeTest1 (test) values (?)");
        for (int i = 0; i < 1000; i++) {
            pstmt.setString(1, "" + i);
            pstmt.addBatch();
        }
        pstmt.executeBatch();
    }
}
