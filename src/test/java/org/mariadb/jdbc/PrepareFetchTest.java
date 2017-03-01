package org.mariadb.jdbc;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

public class PrepareFetchTest extends BaseTest {

    /**
     * Create table and data for following test.
     * @throws SQLException if connection has error
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("prepareFetchSmallTest", "i int");
        String insertQuery = "INSERT INTO prepareFetchSmallTest values ";
        for (int i = 0; i < 20; i++) {
            if (i != 0) insertQuery += ",";
            insertQuery += " (" + i + ")";
        }

        if (testSingleHost) {
            sharedConnection.createStatement().execute(insertQuery);
        }
    }

    @Test
    public void fetchWithEmptyResultPacket() throws SQLException {
        assertTrue(testSingleHost);
        try (Connection connection = setConnection("&useCursorFetch=true&profileSql=true")) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM prepareFetchSmallTest where i < ?")) {

                preparedStatement.setFetchSize(2);
                preparedStatement.setInt(1, 10);
                try (ResultSet rs = preparedStatement.executeQuery()) {
                    for (int i = 0; i < 10; i++) {
                        assertTrue(rs.next());
                        assertEquals(i, rs.getInt(1));
                    }
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void fetchWithPartialResultPacket() throws SQLException {
        assertTrue(testSingleHost);
        try (Connection connection = setConnection("&useCursorFetch=true&profileSql=true")) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM prepareFetchSmallTest where i < ?")) {

                preparedStatement.setFetchSize(3);
                preparedStatement.setInt(1, 10);
                try (ResultSet rs = preparedStatement.executeQuery()) {
                    for (int i = 0; i < 10; i++) {
                        assertTrue(rs.next());
                        assertEquals(i, rs.getInt(1));
                    }
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void fetchConcurrency() throws SQLException {
        assertTrue(testSingleHost);
        try (Connection connection = setConnection("&useCursorFetch=true&profileSql=true")) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM prepareFetchSmallTest where i < ?")) {


                //don't finish reading resultSet before next query
                preparedStatement.setFetchSize(2);
                preparedStatement.setInt(1, 10);
                ResultSet rs = preparedStatement.executeQuery();
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));

                Statement stmt = connection.createStatement();
                stmt.execute("SELECT * FROM prepareFetchSmallTest");

                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test
    public void fetchConcurrencySamePrepare() throws SQLException {
        assertTrue(testSingleHost);
        try (Connection connection = setConnection("&useCursorFetch=true&profileSql=true")) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM prepareFetchSmallTest where i < ?")) {

                //don't finish reading resultSet before next query
                preparedStatement.setFetchSize(2);
                preparedStatement.setInt(1, 10);
                ResultSet rs = preparedStatement.executeQuery();
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));

                preparedStatement.setFetchSize(0);
                preparedStatement.setInt(1, 1);
                ResultSet rs2 = preparedStatement.executeQuery();
                assertTrue(rs2.next());
                assertEquals(0, rs2.getInt(1));
                assertFalse(rs2.next());

                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));

            }

        }
    }

    @Test
    public void fetchConcurrencyWithCache() throws SQLException {
        assertTrue(testSingleHost);
        try (Connection connection = setConnection("&useCursorFetch=true&profileSql=true")) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM prepareFetchSmallTest where i < ?")) {

                //don't finish reading resultSet before next query
                preparedStatement.setFetchSize(2);
                preparedStatement.setInt(1, 10);
                ResultSet rs = preparedStatement.executeQuery();
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));

                try (PreparedStatement preparedStatement2 = connection.prepareStatement("SELECT * FROM prepareFetchSmallTest where i < ?")) {
                    preparedStatement2.setFetchSize(3);
                    preparedStatement2.setInt(1, 10);
                    ResultSet rs2 = preparedStatement2.executeQuery();

                    for (int i = 0; i < 10; i++) {
                        assertTrue(rs2.next());
                        assertEquals(i, rs2.getInt(1));
                    }
                    assertFalse(rs2.next());

                    assertTrue(rs.last());
                    assertEquals(9, rs.getInt(1));
                }
            }
        }
    }

}
