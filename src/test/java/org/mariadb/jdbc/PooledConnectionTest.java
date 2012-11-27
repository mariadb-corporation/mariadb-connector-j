package org.mariadb.jdbc;

import org.junit.Test;

import javax.sql.*;
import java.sql.*;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

class MyEventListener implements ConnectionEventListener,StatementEventListener
{
   public SQLException sqlException;
   public boolean closed;
   public boolean connectionErrorOccured;
   public boolean statementClosed;
   public boolean statementErrorOccured;

   public void connectionClosed(ConnectionEvent event) {
       sqlException = event.getSQLException();
       closed = true;
   }

   public void connectionErrorOccurred(ConnectionEvent event) {
        sqlException = event.getSQLException();
        connectionErrorOccured = true;
   }

   public MyEventListener() {
        sqlException = null;
        closed = false;
        connectionErrorOccured = false;
   }

    public void statementClosed(StatementEvent event) {
        statementClosed = true;
    }

    public void statementErrorOccurred(StatementEvent event) {
        sqlException = event.getSQLException();
        statementErrorOccured = true;
    }
}

public class PooledConnectionTest  {
   @Test
   public void testPooledConnectionClosed() throws Exception  {
       ConnectionPoolDataSource ds = new MySQLDataSource("localhost", 3306, "test");
       PooledConnection pc = ds.getPooledConnection("root","");
       Connection c= pc.getConnection();
       MyEventListener listener = new MyEventListener();
       pc.addConnectionEventListener(listener);
       c.close();
       assertTrue(listener.closed);
       /* Verify physical connection is still ok */
       c.createStatement().execute("select 1");

       /* close physical connection */
       pc.close();
       /* Now verify physical connection is gone */
       try {
            c.createStatement().execute("select 1");
           assertFalse("should never get there", true);
       } catch(Exception e) {

       }
   }

   @Test
   public void testPooledConnectionException() throws Exception {
       ConnectionPoolDataSource ds = new MySQLDataSource("localhost", 3306, "test");
       PooledConnection pc = ds.getPooledConnection("root","");
       MyEventListener listener = new MyEventListener();
       pc.addConnectionEventListener(listener);
       MySQLConnection c = (MySQLConnection)pc.getConnection();

       /* Ask server to abort the connection */
       try {
            c.createStatement().execute("KILL CONNECTION_ID()");
       }
       catch (Exception e) {
         /* exception is expected here, server sends query aborted */
       }

       /* Try to read  after server side closed the connection */
       try {
          c.createStatement().execute("SELECT 1");

          assertTrue("should never get there", false);
       }
       catch (Exception e) {
           /* Check that listener was actually called*/
           assertTrue(listener.sqlException instanceof SQLNonTransientConnectionException);
       }
       pc.close();
       //assertTrue(listener.closed);
   }

    @Test

    public void testPooledConnectionStatementError() throws Exception
    {
       ConnectionPoolDataSource ds = new MySQLDataSource("localhost", 3306, "test");
       PooledConnection pc = ds.getPooledConnection("root","");
       MyEventListener listener = new MyEventListener();
       pc.addStatementEventListener(listener);
       MySQLConnection c = (MySQLConnection)pc.getConnection();
       PreparedStatement ps = c.prepareStatement("zzzz");
       try {
           ps.execute();
           assertTrue("should never get there", false);
       }
       catch(Exception e) {
           assertTrue(listener.statementErrorOccured && listener.sqlException instanceof SQLSyntaxErrorException);
       }
       ps.close();
       assertTrue(listener.statementClosed);
       pc.close();
    }
}
