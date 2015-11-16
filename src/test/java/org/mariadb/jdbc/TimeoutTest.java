package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimeoutTest extends BaseTest {

    private static int selectValue(Connection conn, int value)
            throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select " + value);
            rs.next();
            return rs.getInt(1);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    /**
     * Conj-79.
     *
     * @throws SQLException exception
     */
    @Test
    public void resultSetAfterSocketTimeoutTest() throws SQLException {
        Connection connection = null;
        try {
            try {
                connection = setConnection("&connectTimeout=5&socketTimeout=5");
            } catch (SQLException e) {
                try {
                    //depending on systems, 5 millisecond can be not enougth
                    connection = setConnection("&connectTimeout=50&socketTimeout=50");
                } catch (SQLException ee) {
                    try {
                        connection = setConnection("&connectTimeout=5000&socketTimeout=5000");
                    } catch (SQLException ees) {
                        connection = setConnection("&connectTimeout=50000&socketTimeout=50000");
                    }
                }
            }
            boolean bugReproduced = false;
            int exc = 0;
            int went = 0;
            for (int i = 0; i < 1000; i++) {
                try {
                    int v1 = selectValue(connection, 1);
                    int v2 = selectValue(connection, 2);
                    if (v1 != 1 || v2 != 2) {
                        bugReproduced = true;
                        break;
                    }
                    assertTrue(v1 == 1 && v2 == 2);
                    went++;
                } catch (Exception e) {
                    exc++;
                }
            }
            assertFalse(bugReproduced); // either Exception or fine
            assertTrue(went > 0);
            assertTrue(went + exc == 1000);
        } finally {
            connection.close();
        }
    }

    /**
     * Conj-79.
     *
     * @throws SQLException exception
     */
    @Test
    public void socketTimeoutTest() throws SQLException {
        // set a short connection timeout
        Connection connection = null;
        try {
            connection = setConnection("&connectTimeout=500&socketTimeout=500");
            PreparedStatement ps = connection.prepareStatement("SELECT 1");
            ResultSet rs = ps.executeQuery();
            rs.next();
            logInfo(rs.getString(1));

            // wait for the connection to time out
            ps = connection.prepareStatement("SELECT sleep(1)");

            // a timeout should occur here
            try {
                ps.executeQuery();
                Assert.fail();
            } catch (SQLException e) {
                // check that it's a timeout that occurs
            }

            try {
                ps = connection.prepareStatement("SELECT 2");
                ps.execute();
                Assert.fail("Connection must have thrown error");
            } catch (SQLException e) {
                //normal exception
            }

            // the connection should  be closed
            assertTrue(connection.isClosed());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void waitTimeoutStatementTest() throws SQLException, InterruptedException {
        Connection connection = null;
        try {
            connection = setConnection();
            Statement statement = connection.createStatement();
            statement.execute("set session wait_timeout=1");
            Thread.sleep(2000); // Wait for the server to kill the connection

            logInfo(connection.toString());

            // here a SQLNonTransientConnectionException is expected
            // "Could not read resultset: unexpected end of stream, ..."
            try {
                statement.execute("SELECT 1");
                Assert.fail("Connection must have thrown error");
            } catch (SQLException e) {
                //normal exception
            }

            statement.close();
        } finally {
            connection.close();
        }
    }

    @Test
    public void waitTimeoutResultSetTest() throws SQLException, InterruptedException {
        Connection connection = null;
        try {
            connection = setConnection();
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1");

            rs.next();

            stmt.execute("set session wait_timeout=1");
            Thread.sleep(3000); // Wait for the server to kill the connection

            // here a SQLNonTransientConnectionException is expected
            // "Could not read resultset: unexpected end of stream, ..."
            try {
                rs = stmt.executeQuery("SELECT 2");
                rs.next();
                Assert.fail("Connection must have thrown error");
            } catch (SQLException e) {
                //normal exception
            }
        } finally {
            connection.close();
        }
    }

}