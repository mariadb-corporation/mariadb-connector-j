package org.mariadb.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;

public class CancelTest extends BaseTest {
    @Before
    public void cancelSupported() throws SQLException {
        requireMinimumVersion(5,0);
    }
    @Test(expected = SQLTransientException.class)
    public void cancelTest() throws SQLException{

        Statement stmt = connection.createStatement();
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
                e.printStackTrace();
            }
        }
    }


    @Test (expected = java.sql.SQLTimeoutException.class)
    public void timeoutSleep() throws Exception{
           PreparedStatement stmt = connection.prepareStatement("select sleep(100)");
           stmt.setQueryTimeout(1);
           stmt.execute();
     }

    @Test
    public void NoTimeoutSleep() throws Exception{
        Statement stmt = connection.createStatement();
        stmt.setQueryTimeout(1);
        stmt.execute("select sleep(0.5)");

    }

    @Test
    public void CancelIdleStatement() throws Exception {
        Statement stmt = connection.createStatement();
        stmt.cancel();
        ResultSet rs = stmt.executeQuery("select 1");
        rs.next();
        assertEquals(rs.getInt(1),1);
    }
}
