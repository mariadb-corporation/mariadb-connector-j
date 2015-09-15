package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Test;

import javax.sql.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

class MyEventListener implements ConnectionEventListener, StatementEventListener {
    public SQLException sqlException;
    public boolean closed;
    public boolean connectionErrorOccured;
    public boolean statementClosed;
    public boolean statementErrorOccured;

    public MyEventListener() {
        sqlException = null;
        closed = false;
        connectionErrorOccured = false;
    }

    public void connectionClosed(ConnectionEvent event) {
        sqlException = event.getSQLException();
        closed = true;
    }

    public void connectionErrorOccurred(ConnectionEvent event) {
        sqlException = event.getSQLException();
        connectionErrorOccured = true;
    }

    public void statementClosed(StatementEvent event) {
        statementClosed = true;
    }

    public void statementErrorOccurred(StatementEvent event) {
        sqlException = event.getSQLException();
        statementErrorOccured = true;
    }
}

public class PooledConnectionTest extends BaseTest {
    @Test
    public void testPooledConnectionClosed() throws Exception {
        ConnectionPoolDataSource ds = new MySQLDataSource(hostname, port, database);
        PooledConnection pc = ds.getPooledConnection(username, password);
        Connection c = pc.getConnection();
        MyEventListener listener = new MyEventListener();
        pc.addConnectionEventListener(listener);
        pc.addStatementEventListener(listener);
        c.close();
        Assert.assertTrue(listener.closed);
       /* Verify physical connection is still ok */
        c.createStatement().execute("select 1");

       /* close physical connection */
        pc.close();
       /* Now verify physical connection is gone */
        try {
            c.createStatement().execute("select 1");
            Assert.assertFalse("should never get there", true);
        } catch (Exception e) {

        }
    }

    @Test
    public void testPooledConnectionException() throws Exception {
        ConnectionPoolDataSource ds = new MySQLDataSource(hostname, port, database);
        PooledConnection pc = ds.getPooledConnection(username, password);
        MyEventListener listener = new MyEventListener();
        pc.addConnectionEventListener(listener);
        MySQLConnection c = (MySQLConnection) pc.getConnection();

       /* Ask server to abort the connection */
        try {
            c.createStatement().execute("KILL CONNECTION_ID()");
        } catch (Exception e) {
         /* exception is expected here, server sends query aborted */
        }

       /* Try to read  after server side closed the connection */
        try {
            c.createStatement().execute("SELECT 1");
            Assert.assertTrue("should never get there", false);
        } catch (SQLException e) {
        }
        pc.close();
        //assertTrue(listener.closed);
    }


    @Test
    public void testPooledConnectionStatementError() throws Exception {
        ConnectionPoolDataSource ds = new MySQLDataSource(hostname, port, database);
        PooledConnection pc = ds.getPooledConnection(username, password);
        MyEventListener listener = new MyEventListener();
        pc.addStatementEventListener(listener);
        MySQLConnection c = (MySQLConnection) pc.getConnection();
        PreparedStatement ps = c.prepareStatement("SELECT ?");
        try {
            ps.execute();
            Assert.assertTrue("should never get there", false);
        } catch (Exception e) {
            Assert.assertTrue(listener.statementErrorOccured && listener.sqlException.getSQLState().equals("07004"));
        }
        ps.close();
        Assert.assertTrue(listener.statementClosed);
        pc.close();
    }
}
