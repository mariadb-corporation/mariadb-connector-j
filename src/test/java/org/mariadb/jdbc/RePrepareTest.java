package org.mariadb.jdbc;

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

public class RePrepareTest extends BaseTest {

    @Test
    public void rePrepareTestSelectError() throws SQLException {
        createTable("rePrepareTestSelectError", "test int");
        try (Statement stmt = sharedConnection.createStatement()) {
            stmt.execute("INSERT INTO rePrepareTestSelectError(test) VALUES (1)");
            try (PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT * FROM rePrepareTestSelectError where test = ?")) {
                preparedStatement.setInt(1, 1);
                ResultSet rs = preparedStatement.executeQuery();
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertFalse(rs.next());
                stmt.execute("ALTER TABLE rePrepareTestSelectError" +
                        " CHANGE COLUMN `test` `test` VARCHAR(50) NULL DEFAULT NULL FIRST," +
                        "ADD COLUMN `test2` VARCHAR(50) NULL DEFAULT NULL AFTER `test`;");
                ResultSet rs2 = preparedStatement.executeQuery();
                preparedStatement.setInt(1, 1);
                assertTrue(rs2.next());
                assertEquals("1", rs2.getString(1));
                assertFalse(rs2.next());
            }
        }
    }

    @Test
    public void rePrepareTestInsertError() throws SQLException {
        createTable("rePrepareTestInsertError", "test int");
        try (Statement stmt = sharedConnection.createStatement()) {
            try (PreparedStatement preparedStatement = sharedConnection.prepareStatement("INSERT INTO rePrepareTestInsertError(test) values (?)")) {

                preparedStatement.setInt(1, 1);
                preparedStatement.execute();

                stmt.execute("ALTER TABLE rePrepareTestInsertError" +
                        " CHANGE COLUMN `test` `test` VARCHAR(50) NULL DEFAULT NULL FIRST;");

                preparedStatement.setInt(1, 2);
                preparedStatement.execute();

                stmt.execute("ALTER TABLE rePrepareTestInsertError" +
                        " CHANGE COLUMN `test` `test` VARCHAR(100) NULL DEFAULT NULL FIRST," +
                        "ADD COLUMN `test2` VARCHAR(50) NULL DEFAULT NULL AFTER `test`;");

                stmt.execute("flush tables with read lock");
                stmt.execute("unlock tables");
                preparedStatement.setInt(1, 3);
                preparedStatement.execute();
            }
        }
    }


    @Test
    public void cannotRePrepare() throws SQLException {
        createTable("cannotRePrepare", "test int");
        try (Statement stmt = sharedConnection.createStatement()) {
            try (PreparedStatement preparedStatement = sharedConnection.prepareStatement("INSERT INTO cannotRePrepare(test) values (?)")) {

                preparedStatement.setInt(1, 1);
                preparedStatement.execute();

                stmt.execute("ALTER TABLE cannotRePrepare" +
                        " CHANGE COLUMN `test` `otherName` VARCHAR(50) NULL DEFAULT NULL FIRST;");

                preparedStatement.setInt(1, 2);
                try {
                    preparedStatement.execute();
                    fail();
                } catch (SQLException sqle) {
                    assertTrue(sqle.getMessage().contains("Unknown column 'test' in 'field list'"));
                }

            }
        }
    }
}
