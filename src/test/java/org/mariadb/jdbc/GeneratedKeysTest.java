package org.mariadb.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

public class GeneratedKeysTest extends BaseTest {

    private Statement statement;
    private PreparedStatement preparedStatement;

    @Before
    public void setUp() throws SQLException {
        statement = connection.createStatement();
        statement.execute("DROP TABLE IF EXISTS gen_key_test");
        statement.execute("CREATE TABLE gen_key_test (id INTEGER NOT NULL AUTO_INCREMENT, name VARCHAR(100), PRIMARY KEY (id))");
    }

    @Test
    public void testSimpleGeneratedKeys() throws SQLException {
        statement.executeUpdate("INSERT INTO gen_key_test (id, name) VALUES (null, 'Dave')", Statement.RETURN_GENERATED_KEYS);

        ResultSet resultSet = statement.getGeneratedKeys();
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt(1));
    }

    @Test
    public void testSimpleGeneratedKeysWithPreparedStatement() throws SQLException {
        preparedStatement = connection.prepareStatement(
                "INSERT INTO gen_key_test (id, name) VALUES (null, ?)",
                Statement.RETURN_GENERATED_KEYS);

        preparedStatement.setString(1, "Dave");
        preparedStatement.execute();

        ResultSet resultSet = preparedStatement.getGeneratedKeys();
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt(1));
    }

    @Test
    public void testGeneratedKeysInsertOnDuplicateUpdate() throws SQLException {
        statement.execute("INSERT INTO gen_key_test (id, name) VALUES (null, 'Dave')");

        statement.executeUpdate(
                "INSERT INTO gen_key_test (id, name) VALUES (1, 'Dave') ON DUPLICATE KEY UPDATE id = id",
                Statement.RETURN_GENERATED_KEYS);
        //From the Javadoc: "If this Statement object did not generate any keys, an empty ResultSet object is returned."
        ResultSet resultSet = statement.getGeneratedKeys();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        assertEquals(1, resultSetMetaData.getColumnCount());
        //Since the statement does not generate any keys an empty ResultSet should be returned
        assertFalse(resultSet.next());
    }
}
