package org.mariadb.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

public class GeneratedKeysTest extends BaseTest {
    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("gen_key_test", "id INTEGER NOT NULL AUTO_INCREMENT, name VARCHAR(100), PRIMARY KEY (id)");
        createTable("gen_key_test2", "id INTEGER NOT NULL AUTO_INCREMENT, name VARCHAR(100), PRIMARY KEY (id)");
    }

    @Test
    public void testSimpleGeneratedKeys() throws SQLException {
        Statement statement = sharedConnection.createStatement();
        statement.execute("truncate gen_key_test");
        statement.executeUpdate("INSERT INTO gen_key_test (id, name) VALUES (null, 'Dave')",
                Statement.RETURN_GENERATED_KEYS);

        ResultSet resultSet = statement.getGeneratedKeys();
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt(1));
    }

    @Test
    public void testSimpleGeneratedKeysWithPreparedStatement() throws SQLException {
        sharedConnection.createStatement().execute("truncate gen_key_test");
        PreparedStatement preparedStatement = sharedConnection.prepareStatement(
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
        Statement statement = sharedConnection.createStatement();
        statement.execute("truncate gen_key_test");
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

    /**
     * CONJ-284: Cannot read autoincremented IDs bigger than Short.MAX_VALUE.
     *
     * @throws SQLException exception
     */
    @Test
    public void testGeneratedKeysNegativeValue() throws SQLException {
        Statement statement = sharedConnection.createStatement();
        statement.execute("ALTER TABLE gen_key_test2 AUTO_INCREMENT = 65500");
        PreparedStatement preparedStatement = sharedConnection.prepareStatement("INSERT INTO gen_key_test2 (name) VALUES (?)");
        preparedStatement.setString(1, "t");
        preparedStatement.execute();
        ResultSet rs = preparedStatement.getGeneratedKeys();
        rs.next();
        assertEquals(65500, rs.getInt(1));
    }

}
