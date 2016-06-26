package org.mariadb.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class ScrollTypeTest extends BaseTest {
    /**
     * Data initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("resultsSetReadingTest", "id int not null primary key auto_increment, test int");
        if (testSingleHost) {
            Statement st = sharedConnection.createStatement();
            st.execute("INSERT INTO resultsSetReadingTest (test) values (1), (2), (3)");
        }
    }

    @Test
    public void scrollInsensitivePrepareStmt() throws SQLException {
        try (PreparedStatement stmt = sharedConnection.prepareStatement("SELECT * FROM resultsSetReadingTest",
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(2);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.beforeFirst();
            } catch (SQLException sqle) {
                fail("beforeFirst() should work on a TYPE_SCROLL_INSENSITIVE result set");
            }
        }
    }

    @Test
    public void scrollInsensitiveStmt() throws SQLException {
        try (Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(2);
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM resultsSetReadingTest")) {
                rs.beforeFirst();
            } catch (SQLException sqle) {
                fail("beforeFirst() should work on a TYPE_SCROLL_INSENSITIVE result set");
            }
        }
    }

    @Test(expected = SQLException.class)
    public void scrollForwardOnlyPrepareStmt() throws SQLException {
        try (PreparedStatement stmt = sharedConnection.prepareStatement("SELECT * FROM resultsSetReadingTest",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(2);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.beforeFirst();
                fail("beforeFirst() shouldn't work on a TYPE_FORWARD_ONLY result set");
            }
        }
    }

    @Test(expected = SQLException.class)
    public void scrollForwardOnlyStmt() throws SQLException {
        try (Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(2);
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM resultsSetReadingTest")) {
                rs.beforeFirst();
                fail("beforeFirst() shouldn't work on a TYPE_FORWARD_ONLY result set");
            }
        }
    }
}
