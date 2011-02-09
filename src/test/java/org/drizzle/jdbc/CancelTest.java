package org.drizzle.jdbc;

import org.drizzle.jdbc.exception.SQLQueryTimedOutException;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;

public class CancelTest {
    @Test
    public void cancelQuery() throws SQLException, InterruptedException {
        Connection conn = DriverManager.getConnection("jdbc:drizzle://"+DriverTest.host+":3306/test_units_jdbc");
        Statement stmt = conn.createStatement();

        new QueryThread(stmt).start();
        Thread.sleep(1000);
        stmt.cancel();
        Thread.sleep(100); // need to wait for server to properly finish - not likely in a real app.
// verify that the connection is still valid:
        ResultSet rs = stmt.executeQuery("SELECT 1 FROM DUAL");
        assertTrue(rs.next());
    }
    private static class QueryThread extends Thread {
        private final Statement stmt;

        public QueryThread(Statement stmt) {
            this.stmt = stmt;
        }
        @Override
        public void run() {
            try {
                stmt.execute("select sleep(1000)"); // seconds

            } catch (SQLException e) {
                System.out.println(e.getSQLState());
                //e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    @Test(expected = SQLQueryTimedOutException.class)
    public void timeoutQuery() throws SQLException, InterruptedException {
        Connection conn = DriverManager.getConnection("jdbc:drizzle://"+DriverTest.host+":3306/test_units_jdbc");
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(1);
        stmt.executeQuery("SELECT sleep(10)");

    }
    @Test(expected = SQLQueryTimedOutException.class)
    public void timeoutPrepQuery() throws SQLException, InterruptedException {
        Connection conn = DriverManager.getConnection("jdbc:drizzle://"+DriverTest.host+":3306/test_units_jdbc");
        PreparedStatement stmt = conn.prepareStatement("select sleep(10)");
        stmt.setQueryTimeout(1);
        stmt.execute();

    }
}
