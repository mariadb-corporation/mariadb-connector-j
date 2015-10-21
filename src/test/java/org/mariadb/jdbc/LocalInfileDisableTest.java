package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class LocalInfileDisableTest extends BaseTest {
    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("t", "id int, test varchar(100)");
    }

    @Test
    public void testLocalInfileWithoutInputStream() throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection("&allowLocalInfile=false");
            Statement stmt = null;
            Exception ex = null;
            try {
                stmt = connection.createStatement();
                stmt.executeUpdate("LOAD DATA LOCAL INFILE 'dummy.tsv' INTO TABLE t (id, test)");
            } catch (Exception e) {
                ex = e;
            } finally {
                try {
                    stmt.close();
                } catch (Exception ignore) {
                    //eat exception
                }
            }

            Assert.assertNotNull("Expected an exception to be thrown", ex);
            String message = ex.getMessage();
            String expectedMessage = "Usage of LOCAL INFILE is disabled. To use it enable it via the connection "
                    + "property allowLocalInfile=true";
            Assert.assertEquals(message, expectedMessage);
        } finally {
            connection.close();
        }
    }


}
