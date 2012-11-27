package org.mariadb.jdbc;

import org.junit.Test;
import org.mariadb.jdbc.exception.SQLQueryCancelledException;
import org.mariadb.jdbc.exception.SQLQueryTimedOutException;

import java.sql.*;

public class CancelTest extends BaseTest {
    @Test(expected = SQLQueryCancelledException.class)
    public void cancelQuery() throws SQLException, InterruptedException {

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
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }


    @Test(expected = SQLQueryTimedOutException.class)
    public void timeoutSleep() throws Exception{
           PreparedStatement stmt = connection.prepareStatement("select sleep(2)");
           stmt.setQueryTimeout(1);
           stmt.execute();
     }

    @Test
    public void NoTimeoutSleep() throws Exception{
        Statement stmt = connection.createStatement();
        stmt.setQueryTimeout(1);
        stmt.execute("select sleep(0.5)");

    }
}
