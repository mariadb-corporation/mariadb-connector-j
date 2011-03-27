package org.drizzle.jdbc;

import org.drizzle.jdbc.exception.SQLQueryCancelledException;
import org.drizzle.jdbc.exception.SQLQueryTimedOutException;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;

public class CancelTest {
    @Test(expected = SQLQueryCancelledException.class)
    public void cancelQuery() throws SQLException, InterruptedException {
        Connection conn = DriverManager.getConnection("jdbc:drizzle://"+DriverTest.host+":3306/test_units_jdbc");
        Statement stmt = conn.createStatement();
         new CancelThread(stmt).start();
        stmt.execute("select * from information_schema.columns, information_schema.tables, information_schema.table_constraints");

    }
    private static class CancelThread extends Thread {
        private final Statement stmt;

        public CancelThread(Statement stmt) {
            this.stmt = stmt;
        }
        @Override
        public void run() {
            try {
                Thread.sleep(1000);

                stmt.cancel();

            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }

    @Test(expected = SQLQueryTimedOutException.class)
    public void timeoutQuery() throws SQLException, InterruptedException {
        Connection conn = DriverManager.getConnection("jdbc:drizzle://"+DriverTest.host+":3306/test_units_jdbc");
        Statement stmt = conn.createStatement();
        stmt.setQueryTimeout(1);
        stmt.executeQuery("select * from information_schema.columns, information_schema.tables, information_schema.table_constraints");

    }
    @Test(expected = SQLQueryTimedOutException.class)
    public void timeoutPrepQuery() throws SQLException, InterruptedException {
        Connection conn = DriverManager.getConnection("jdbc:drizzle://"+DriverTest.host+":3306/test_units_jdbc");
        PreparedStatement stmt = conn.prepareStatement("select * from information_schema.columns, information_schema.tables, information_schema.table_constraints");
        stmt.setQueryTimeout(1);
        stmt.execute();
    }
    @Test(expected = SQLNonTransientException.class)
    public void connectionTimeout() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:drizzle://www.google.com:1234/test_units_jdbc?connectTimeout=1");
    }

    
}
