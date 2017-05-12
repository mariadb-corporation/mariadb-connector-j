package org.mariadb.jdbc;

import org.junit.Assume;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

public class StateChangeTest extends BaseTest {

    @Test
    public void databaseStateChange() throws SQLException {
        Assume.assumeTrue((isMariadbServer() && minVersion(10, 2))
                || (!isMariadbServer() && minVersion(5, 7)));
        try (Connection connection = setConnection()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("drop database if exists " + MariaDbConnection.quoteIdentifier("_test_db"));
                stmt.execute("create database " + MariaDbConnection.quoteIdentifier("_test_db"));
                assertEquals(database, connection.getCatalog());
                stmt.execute("USE " + MariaDbConnection.quoteIdentifier("_test_db"));
                assertEquals("_test_db", connection.getCatalog());
            }
        }
    }

    @Test
    public void timeZoneChange() throws SQLException {
        Assume.assumeTrue((isMariadbServer() && minVersion(10, 2))
                || (!isMariadbServer() && minVersion(5, 7)));
        try (Connection connection = setConnection()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("drop database if exists " + MariaDbConnection.quoteIdentifier("_test_db"));
                stmt.execute("create database " + MariaDbConnection.quoteIdentifier("_test_db"));
                assertEquals(database, connection.getCatalog());
                stmt.execute("USE " + MariaDbConnection.quoteIdentifier("_test_db"));
                assertEquals("_test_db", connection.getCatalog());
            }
        }
    }


    @Test
    public void autocommitChange() throws SQLException {
        try (Connection connection = setConnection()) {
            try (Statement stmt = connection.createStatement()) {
                assertTrue(connection.getAutoCommit());
                stmt.execute("SET autocommit=false");
                assertFalse(connection.getAutoCommit());
            }
        }
    }

    @Test
    public void autoIncrementChange() throws SQLException {
        Assume.assumeTrue((isMariadbServer() && minVersion(10, 2))
                || (!isMariadbServer() && minVersion(5, 7)));
        createTable("autoIncrementChange", "id int not null primary key auto_increment, name char(20)");
        try (Connection connection = setConnection()) {
            try (Statement stmt = connection.createStatement()) {
                try (PreparedStatement preparedStatement =
                             connection.prepareStatement("INSERT INTO autoIncrementChange(name) value (?)",
                                     Statement.RETURN_GENERATED_KEYS)) {

                    preparedStatement.setString(1, "a");
                    preparedStatement.execute();
                    ResultSet rs = preparedStatement.getGeneratedKeys();
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));

                    preparedStatement.setString(1, "b");
                    preparedStatement.execute();
                    rs = preparedStatement.getGeneratedKeys();
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));

                    stmt.execute("SET @@session.auto_increment_increment=10");
                    ResultSet rs2 = stmt.executeQuery("SHOW VARIABLES WHERE Variable_name like 'auto_increment_increment'");
                    assertTrue(rs2.next());
                    assertEquals(10, rs2.getInt(2));

                    preparedStatement.setString(1, "c");
                    preparedStatement.execute();
                    rs = preparedStatement.getGeneratedKeys();
                    assertTrue(rs.next());
                    assertEquals(11, rs.getInt(1));

                    rs2 = stmt.executeQuery("select * from autoIncrementChange");
                    assertTrue(rs2.next());
                    assertEquals("a", rs2.getString(2));
                    assertEquals(1, rs2.getInt(1));
                    assertTrue(rs2.next());
                    assertEquals("b", rs2.getString(2));
                    assertEquals(2, rs2.getInt(1));
                    assertTrue(rs2.next());
                    assertEquals("c", rs2.getString(2));
                    assertEquals(11, rs2.getInt(1));
                    assertFalse(rs2.next());
                }
            }
        }
    }


}
