package org.mariadb.jdbc;

import org.junit.Test;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PooledConnectionTest extends BaseTest {

    @Test(expected = SQLException.class)
    public void testPooledConnectionClosed() throws Exception {
        ConnectionPoolDataSource ds = new MariaDbDataSource(hostname != null ? hostname : "localhost", port, database);
        PooledConnection pc = ds.getPooledConnection(username, password);
        Connection connection = pc.getConnection();
        MyEventListener listener = new MyEventListener();
        pc.addConnectionEventListener(listener);
        pc.addStatementEventListener(listener);
        connection.close();
        assertTrue(listener.closed);
        /* Verify physical connection is still ok */
        connection.createStatement().execute("select 1");

        /* close physical connection */
        pc.close();
        /* Now verify physical connection is gone */
        connection.createStatement().execute("select 1");
        fail("should never get there : previous must have thrown exception");
    }

    @Test(expected = SQLException.class)
    public void testPooledConnectionException() throws Exception {
        ConnectionPoolDataSource ds = new MariaDbDataSource(hostname != null ? hostname : "localhost", port, database);
        PooledConnection pc = null;
        try {
            pc = ds.getPooledConnection(username, password);
            MyEventListener listener = new MyEventListener();
            pc.addConnectionEventListener(listener);
            MariaDbConnection connection = (MariaDbConnection) pc.getConnection();

            /* Ask server to abort the connection */
            try {
                connection.createStatement().execute("KILL CONNECTION_ID()");
            } catch (Exception e) {
                /* exception is expected here, server sends query aborted */
            }

            /* Try to read  after server side closed the connection */
            connection.createStatement().execute("SELECT 1");
            fail("should never get there");
        } finally {
            if (pc != null) pc.close();
        }
    }


    @Test
    public void testPooledConnectionStatementError() throws Exception {
        ConnectionPoolDataSource ds = new MariaDbDataSource(hostname != null ? hostname : "localhost", port, database);
        PooledConnection pc = ds.getPooledConnection(username, password);
        MyEventListener listener = new MyEventListener();
        pc.addStatementEventListener(listener);
        MariaDbConnection connection = (MariaDbConnection) pc.getConnection();
        try (PreparedStatement ps = connection.prepareStatement("SELECT ?")) {
            ps.execute();
            assertTrue("should never get there", false);
        } catch (Exception e) {
            assertTrue(listener.statementErrorOccured);
            if (sharedBulkCapacity()) {
                assertTrue(e.getMessage().contains("Parameter at position 1 is not set")
                        || e.getMessage().contains("Incorrect arguments to mysqld_stmt_execute"));
            } else {
                //HY000 if server >= 10.2 ( send prepare and query in a row), 07004 otherwise
                assertTrue("07004".equals(listener.sqlException.getSQLState()) || "HY000".equals(listener.sqlException.getSQLState()));
            }
        }
        assertTrue(listener.statementClosed);
        pc.close();
    }
}
