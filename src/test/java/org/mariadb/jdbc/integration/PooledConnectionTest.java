/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.*;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.MariaDbDataSource;

public class PooledConnectionTest extends Common {

  @Test
  public void testPooledConnectionClosed() throws Exception {
    ConnectionPoolDataSource ds = new MariaDbDataSource(mDefUrl);
    PooledConnection pc = ds.getPooledConnection();
    Connection connection = pc.getConnection();
    MyEventListener listener = new MyEventListener();
    pc.addConnectionEventListener(listener);
    pc.addStatementEventListener(listener);
    connection.close();
    assertTrue(listener.closed);
    assertThrows(SQLException.class, () -> connection.createStatement().execute("select 1"));
    pc.removeConnectionEventListener(listener);
    pc.removeStatementEventListener(listener);
  }

  @Test
  public void testPooledConnectionException() throws Exception {
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));

    ConnectionPoolDataSource ds = new MariaDbDataSource(mDefUrl);
    PooledConnection pc = null;
    try {
      pc = ds.getPooledConnection();
      MyEventListener listener = new MyEventListener();
      pc.addConnectionEventListener(listener);
      Connection connection = pc.getConnection();

      /* Ask server to abort the connection */
      try {
        connection.createStatement().execute("KILL CONNECTION_ID()");
      } catch (Exception e) {
        /* exception is expected here, server sends query aborted */
      }

      /* Try to read  after server side closed the connection */
      assertThrows(SQLException.class, () -> connection.createStatement().execute("SELECT 1"));
    } finally {
      if (pc != null) {
        pc.close();
      }
    }
  }

  @Test
  public void testPooledConnectionStatementError() throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP USER IF EXISTS 'dsUser'");
    if (minVersion(8, 0, 0)) {
      if (isMariaDBServer()) {
        stmt.execute("CREATE USER 'dsUser'@'%' IDENTIFIED BY 'MySup8%rPassw@ord'");
        stmt.execute("GRANT SELECT ON " + sharedConn.getCatalog() + ".* TO 'dsUser'@'%'");
      } else {
        stmt.execute(
            "CREATE USER 'dsUser'@'%' IDENTIFIED WITH mysql_native_password BY 'MySup8%rPassw@ord'");
        stmt.execute("GRANT SELECT ON " + sharedConn.getCatalog() + ".* TO 'dsUser'@'%'");
      }
    } else {
      stmt.execute("CREATE USER 'dsUser'@'%'");
      stmt.execute(
          "GRANT SELECT ON "
              + sharedConn.getCatalog()
              + ".* TO 'dsUser'@'%' IDENTIFIED BY 'MySup8%rPassw@ord'");
    }
    stmt.execute("FLUSH PRIVILEGES");

    ConnectionPoolDataSource ds = new MariaDbDataSource(mDefUrl);
    PooledConnection pc = ds.getPooledConnection("dsUser", "MySup8%rPassw@ord");
    MyEventListener listener = new MyEventListener();
    pc.addStatementEventListener(listener);
    Connection connection = pc.getConnection();
    try (PreparedStatement ps = connection.prepareStatement("SELECT ?")) {
      ps.execute();
      fail("should never get there");
    } catch (Exception e) {
      assertTrue(listener.statementErrorOccured);
    }
    assertTrue(listener.statementClosed);
    pc.close();
  }

  public class MyEventListener implements ConnectionEventListener, StatementEventListener {

    public SQLException sqlException;
    public boolean closed;
    public boolean connectionErrorOccured;
    public boolean statementClosed;
    public boolean statementErrorOccured;

    /** MyEventListener initialisation. */
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
}
