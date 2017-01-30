package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

public class ResultSetTest extends BaseTest {
    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("result_set_test", "id int not null primary key auto_increment, name char(20)");
    }

    @Test
    public void isBeforeFirstFetchTest() throws SQLException {
        insertRows(1);
        Statement statement = sharedConnection.createStatement();
        statement.setFetchSize(1);
        ResultSet resultSet = statement.executeQuery("SELECT * FROM result_set_test");
        Assert.assertTrue(resultSet.isBeforeFirst());
        while (resultSet.next()) {
            Assert.assertFalse(resultSet.isBeforeFirst());
        }
        Assert.assertFalse(resultSet.isBeforeFirst());
        resultSet.close();
        try {
            resultSet.isBeforeFirst();
            Assert.fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            //Make sure an exception has been thrown informing us that the ResultSet was closed
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void isBeforeFirstFetchZeroRowsTest() throws SQLException {
        insertRows(0);
        Statement statement = sharedConnection.createStatement();
        statement.setFetchSize(1);
        ResultSet resultSet = statement.executeQuery("SELECT * FROM result_set_test");
        Assert.assertFalse(resultSet.isBeforeFirst());
        resultSet.next();
        Assert.assertFalse(resultSet.isBeforeFirst());
        resultSet.close();
        try {
            resultSet.isBeforeFirst();
            Assert.fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            //Make sure an exception has been thrown informing us that the ResultSet was closed
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void isClosedTest() throws SQLException {
        insertRows(1);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isClosed());
        while (resultSet.next()) {
            assertFalse(resultSet.isClosed());
        }
        assertFalse(resultSet.isClosed());
        resultSet.close();
        assertTrue(resultSet.isClosed());
    }

    @Test
    public void isBeforeFirstTest() throws SQLException {
        insertRows(1);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertTrue(resultSet.isBeforeFirst());
        while (resultSet.next()) {
            assertFalse(resultSet.isBeforeFirst());
        }
        assertFalse(resultSet.isBeforeFirst());
        resultSet.close();
        try {
            resultSet.isBeforeFirst();
            fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            //Make sure an exception has been thrown informing us that the ResultSet was closed
            assertTrue(e.getMessage().contains("closed"));
        }

    }

    @Test
    public void isFirstZeroRowsTest() throws SQLException {
        insertRows(0);
        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet resultSet = stmt.executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isFirst());
        assertFalse(resultSet.next()); //No more rows after this
        assertFalse(resultSet.isFirst()); // connectorj compatibility
        assertFalse(resultSet.first());
        resultSet.close();
        try {
            resultSet.isFirst();
            fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            //Make sure an exception has been thrown informing us that the ResultSet was closed
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void isFirstTwoRowsTest() throws SQLException {
        insertRows(2);
        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet resultSet = stmt.executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isFirst());
        resultSet.next();
        assertTrue(resultSet.isFirst());
        resultSet.next();
        assertFalse(resultSet.isFirst());
        resultSet.next(); //No more rows after this
        assertFalse(resultSet.isFirst());
        assertTrue(resultSet.first());
        assertEquals(1, resultSet.getInt(1));
        resultSet.close();
        try {
            resultSet.isFirst();
            fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            //Make sure an exception has been thrown informing us that the ResultSet was closed
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void isLastZeroRowsTest() throws SQLException {
        insertRows(0);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isLast()); // connectorj compatibility
        resultSet.next(); //No more rows after this
        assertFalse(resultSet.isLast());
        assertFalse(resultSet.last());
        resultSet.close();
        try {
            resultSet.isLast();
            fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            //Make sure an exception has been thrown informing us that the ResultSet was closed
            assertTrue(e.getMessage().contains("closed"));
        }
    }


    @Test
    public void isLastTwoRowsTest() throws SQLException {
        insertRows(2);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isLast());
        resultSet.next();
        assertFalse(resultSet.isLast());
        resultSet.next();
        assertTrue(resultSet.isLast());
        resultSet.next(); //No more rows after this
        assertFalse(resultSet.isLast());
        assertTrue(resultSet.last());
        assertEquals(2, resultSet.getInt(1));
        resultSet.close();
        try {
            resultSet.isLast();
            fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            //Make sure an exception has been thrown informing us that the ResultSet was closed
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void isAfterLastZeroRowsTest() throws SQLException {
        insertRows(0);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isAfterLast());
        resultSet.next(); //No more rows after this
        assertFalse(resultSet.isAfterLast());
        resultSet.close();
        try {
            resultSet.isAfterLast();
            fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            //Make sure an exception has been thrown informing us that the ResultSet was closed
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void isAfterLastTwoRowsTest() throws SQLException {
        insertRows(2);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isAfterLast());
        resultSet.next();
        assertFalse(resultSet.isAfterLast());
        resultSet.next();
        assertFalse(resultSet.isAfterLast());
        resultSet.next(); //No more rows after this
        assertTrue(resultSet.isAfterLast());
        assertTrue(resultSet.last());
        assertEquals(2, resultSet.getInt(1));
        resultSet.close();
        try {
            resultSet.isAfterLast();
            fail("The above row should have thrown an SQLException");
        } catch (SQLException e) {
            //Make sure an exception has been thrown informing us that the ResultSet was closed
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void previousTest() throws SQLException {
        insertRows(2);
        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("SELECT * FROM result_set_test");
        assertFalse(rs.previous());
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.previous());
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.previous());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.last());
        assertEquals(2, rs.getInt(1));
        rs.close();
    }

    @Test
    public void firstTest() throws SQLException {
        insertRows(2);
        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("SELECT * FROM result_set_test");
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.first());
        assertTrue(rs.isFirst());
        rs.close();
        try {
            rs.first();
            fail("cannot call first() on a closed result set");
        } catch (SQLException sqlex) {
            //eat exception
        }
    }

    @Test
    public void lastTest() throws SQLException {
        insertRows(2);
        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("SELECT * FROM result_set_test");
        assertTrue(rs.last());
        assertTrue(rs.isLast());
        assertFalse(rs.next());
        rs.first();
        rs.close();
        try {
            rs.last();
            fail("cannot call last() on a closed result set");
        } catch (SQLException sqlex) {
            //eat exception
        }
    }

    private void insertRows(int numberOfRowsToInsert) throws SQLException {
        sharedConnection.createStatement().execute("truncate result_set_test ");
        for (int i = 1; i <= numberOfRowsToInsert; i++) {
            sharedConnection.createStatement().executeUpdate("INSERT INTO result_set_test VALUES(" + i
                    + ", 'row" + i + "')");
        }
    }

    @Test
    public void testResultSetAbsolute() throws Exception {
        insertRows(50);
        try (Statement statement = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            statement.setFetchSize(10);
            try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {
                assertFalse(rs.absolute(52));
                assertFalse(rs.absolute(-52));

                assertTrue(rs.absolute(42));
                assertEquals("row42", rs.getString(2));

                assertTrue(rs.absolute(-11));
                assertEquals("row40", rs.getString(2));

                assertTrue(rs.absolute(0));
                assertTrue(rs.isBeforeFirst());

                assertFalse(rs.absolute(51));
                assertTrue(rs.isAfterLast());

                assertTrue(rs.absolute(-1));
                assertEquals("row50", rs.getString(2));

                assertTrue(rs.absolute(-50));
                assertEquals("row1", rs.getString(2));
            }
        }
    }

    @Test
    public void testResultSetIsAfterLast() throws Exception {
        insertRows(2);
        try (Statement statement = sharedConnection.createStatement()) {
            statement.setFetchSize(1);
            try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {
                assertFalse(rs.isLast());
                assertFalse(rs.isAfterLast());
                assertTrue(rs.next());
                assertFalse(rs.isLast());
                assertFalse(rs.isAfterLast());
                assertTrue(rs.next());
                assertTrue(rs.isLast());
                assertFalse(rs.isAfterLast());
                assertFalse(rs.next());
                assertFalse(rs.isLast());
                assertTrue(rs.isAfterLast());
            }

            insertRows(0);
            try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {
                assertFalse(rs.isAfterLast());
                assertFalse(rs.isLast());
                assertFalse(rs.next());
                assertFalse(rs.isLast());
                assertFalse(rs.isAfterLast()); //jdbc indicate that results with no rows return false.
            }
        }
    }


    @Test
    public void testResultSetAfterLast() throws Exception {
        try (Statement statement = sharedConnection.createStatement()) {
            checkLastResultSet(statement);
            statement.setFetchSize(1);
            checkLastResultSet(statement);

        }
    }

    private void checkLastResultSet(Statement statement) throws SQLException {

        insertRows(10);
        try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {

            assertTrue(rs.last());
            assertFalse(rs.isAfterLast());
            assertTrue(rs.isLast());

            rs.afterLast();
            assertTrue(rs.isAfterLast());
            assertFalse(rs.isLast());

        }

        insertRows(0);
        try (ResultSet rs = statement.executeQuery("SELECT * FROM result_set_test")) {

            assertFalse(rs.last());
            assertFalse(rs.isAfterLast());
            assertFalse(rs.isLast());

            rs.afterLast();
            assertFalse(rs.isAfterLast()); //jdbc indicate that results with no rows return false.
            assertFalse(rs.isLast());
        }

    }
}
