package org.mariadb.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

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
            assertTrue(e instanceof SQLException);
        }
    }

    @Test
    public void isFirstZeroRowsTest() throws SQLException {
        insertRows(0);
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isFirst());
        assertFalse(resultSet.next()); //No more rows after this
        assertFalse(resultSet.isFirst()); // connectorj compatibility
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
        ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(resultSet.isFirst());
        resultSet.next();
        assertTrue(resultSet.isFirst());
        resultSet.next();
        assertFalse(resultSet.isFirst());
        resultSet.next(); //No more rows after this
        assertFalse(resultSet.isFirst());
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
        ResultSet rs = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
        assertFalse(rs.previous());
        assertTrue(rs.next());
        assertFalse(rs.previous());
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.previous());
        rs.close();
    }

    @Test
    public void firstTest() throws SQLException {
        insertRows(2);
        ResultSet rs = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
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
        ResultSet rs = sharedConnection.createStatement().executeQuery("SELECT * FROM result_set_test");
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
}
