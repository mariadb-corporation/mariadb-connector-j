package org.mariadb.jdbc;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

public class TruncateExceptionTest extends BaseTest {
    /**
     * Tables initialisation.
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("TruncateExceptionTest", "id tinyint");
        createTable("TruncateExceptionTest2", "id tinyint not null primary key auto_increment, id2 tinyint ");

    }

    @Test
    public void truncationThrowError() throws SQLException {
        try {
            queryTruncation(true);
            fail("Must have thrown SQLException");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void truncationThrowNoError() throws SQLException {
        try {
            ResultSet resultSet = sharedConnection.createStatement().executeQuery("SELECT @@sql_mode");
            resultSet.next();
            //if server is already throwing truncation, cancel test
            Assume.assumeFalse(resultSet.getString(1).contains("STRICT_TRANS_TABLES"));

            queryTruncation(false);
        } catch (SQLException e) {
            e.printStackTrace();

            fail("Must not have thrown exception");
        }
    }

    /**
     * Execute a query with truncated data.
     * @param truncation connection parameter.
     * @throws SQLException if SQLException occur
     */
    public void queryTruncation(boolean truncation) throws SQLException {
        try (Connection connection = setConnection("&jdbcCompliantTruncation=" + truncation)) {
            Statement stmt = connection.createStatement();
            stmt.execute("INSERT INTO TruncateExceptionTest (id) VALUES (999)");
            stmt.close();
        }
    }


    @Test
    public void queryTruncationFetch() throws SQLException {
        try (Connection connection = setConnection("&jdbcCompliantTruncation=true")) {
            Statement stmt = connection.createStatement();
            for (int i = 0 ; i < 10; i++    ) {
                stmt.execute("INSERT INTO TruncateExceptionTest2 (id2) VALUES (" + i + ")");
            }
            stmt.setFetchSize(1);
            PreparedStatement pstmt = connection.prepareStatement("INSERT INTO TruncateExceptionTest2 (id2) VALUES (?)");
            pstmt.setInt(1, 45);
            pstmt.addBatch();
            pstmt.setInt(1, 999);
            pstmt.addBatch();
            pstmt.setInt(1, 55);
            pstmt.addBatch();
            try {
                pstmt.executeBatch();
                fail("Must have thrown SQLException");
            } catch (SQLException e) {
            }
            //resultset must have been fetch
            ResultSet rs = pstmt.getGeneratedKeys();
            if (sharedIsRewrite()) {
                assertFalse(rs.next());
            } else {
                assertTrue(rs.next());
                System.out.println(rs.getInt(1));
            }
        }
    }
}
