package org.drizzle.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
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
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }



}
