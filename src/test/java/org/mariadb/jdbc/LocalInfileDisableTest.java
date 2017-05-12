package org.mariadb.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LocalInfileDisableTest extends BaseTest {
    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("t", "id int, test varchar(100)");
    }

    @Test
    public void testLocalInfileWithoutInputStream() throws SQLException {
        try (Connection connection = setConnection("&allowLocalInfile=false")) {
            Exception ex = null;
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("LOAD DATA LOCAL INFILE 'dummy.tsv' INTO TABLE t (id, test)");
            } catch (Exception e) {
                ex = e;
            }

            assertNotNull("Expected an exception to be thrown", ex);
            String message = ex.getMessage();
            String expectedMessage = "Usage of LOCAL INFILE is disabled. To use it enable it via the connection property allowLocalInfile=true";
            assertTrue(message.contains(expectedMessage));
        }
    }


}
