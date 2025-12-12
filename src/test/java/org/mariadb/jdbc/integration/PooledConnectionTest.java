// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.MariaDbDataSource;
import org.mariadb.jdbc.MariaDbPoolDataSource;
import org.mariadb.jdbc.export.SslMode;
import org.mariadb.jdbc.integration.tools.TcpProxy;
import org.mariadb.jdbc.pool.MariaDbInnerPoolConnection;
import org.mariadb.jdbc.pool.Pool;
import org.mariadb.jdbc.pool.Pools;

public class PooledConnectionTest extends Common {

  @Test
  public void testPooledConnectionClosed() throws Exception {
    ConnectionPoolDataSource ds = new MariaDbDataSource(mDefUrl);
    PooledConnection pc = ds.getPooledConnection();
    Connection connection = pc.getConnection();
    MyEventListener listener = new MyEventListener();
    pc.addConnectionEventListener(listener);
    pc.addStatementEventListener(listener);
    pc.close();
    assertTrue(listener.closed);
    assertThrows(SQLException.class, () -> connection.createStatement().execute("select 1"));
    pc.removeConnectionEventListener(listener);
    pc.removeStatementEventListener(listener);
  }

  @Test
  public void testPoolWait() throws Exception {
    try (MariaDbPoolDataSource ds =
        new MariaDbPoolDataSource(
            mDefUrl + "&sessionVariables=wait_timeout=1&maxIdleTime=2&testMinRemovalDelay=2")) {
      Thread.sleep(4000);
      PooledConnection pc = ds.getPooledConnection();
      pc.getConnection().isValid(1);
      pc.close();
    }
  }

  @Test
  public void testPoolWaitWithValidation() throws Exception {
    try (MariaDbPoolDataSource ds = new MariaDbPoolDataSource(mDefUrl + "&poolValidMinDelay=1")) {
      Thread.sleep(100);
      PooledConnection pc = ds.getPooledConnection();
      pc.getConnection().isValid(1);
      pc.close();
    }
  }

  @Test
  public void testPoolFailover() throws Exception {
    Configuration conf = Configuration.parse(mDefUrl);
    HostAddress hostAddress = conf.addresses().get(0);
    try {
      proxy = new TcpProxy(hostAddress.host, hostAddress.port);
    } catch (IOException i) {
      throw new SQLException("proxy error", i);
    }

    String url =
        mDefUrl.replaceAll(
            "//(" + hostname + "|" + hostname + ":" + port + ")/",
            "//localhost:" + proxy.getLocalPort() + "/");
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    try (MariaDbPoolDataSource ds =
        new MariaDbPoolDataSource(url + "&poolValidMinDelay=1&connectTimeout=500&maxPoolSize=1")) {

      PooledConnection pc = ds.getPooledConnection();
      pc.getConnection().isValid(1);
      pc.close();
      Thread.sleep(200);
      proxy.stop();
      Common.assertThrowsContains(
          SQLException.class,
          ds::getPooledConnection,
          "No connection available within the specified time");
    }
  }

  @Test
  public void testPoolKillConnection() throws Exception {
    Assumptions.assumeTrue(!isMaxscale());

    File.createTempFile("log", ".tmp");

    try (MariaDbPoolDataSource ds =
        new MariaDbPoolDataSource(mDefUrl + "&maxPoolSize=1&allowPublicKeyRetrieval")) {
      Thread.sleep(100);
      MariaDbInnerPoolConnection pc = (MariaDbInnerPoolConnection) ds.getPooledConnection();
      org.mariadb.jdbc.Connection conn = pc.getConnection();
      long threadId = conn.getThreadId();
      try {
        conn.createStatement().execute("KILL " + threadId);
      } catch (SQLException e) {
        // eat "Connection was killed" message
      }
      pc.close();
      pc = (MariaDbInnerPoolConnection) ds.getPooledConnection();
      conn = pc.getConnection();
      assertNotEquals(threadId, conn.getThreadId());
      pc.close();
    }
  }

  @Test
  public void testPooledConnectionException() throws Exception {
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
  public void testPooledConnectionException2() throws Exception {
    try (Pool pool = Pools.retrievePool(Configuration.parse(mDefUrl + "&maxPoolSize=2"))) {
      MariaDbInnerPoolConnection pooledConnection = pool.getPoolConnection();
      org.mariadb.jdbc.Connection con = pooledConnection.getConnection();
      con.setAutoCommit(false);
      con.createStatement().execute("START TRANSACTION ");

      Connection con2 = pool.getPoolConnection().getConnection();
      con2.createStatement().execute("KILL " + con.getThreadId());
      con2.close();
      Thread.sleep(10);
      assertThrows(SQLException.class, con::commit);
      pooledConnection.close();
    }
  }

  @Test
  public void testPooledConnectionStatementError() throws Exception {
    Assumptions.assumeTrue(!isMaxscale());
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP USER IF EXISTS 'StatementErrorUser'" + getHostSuffix());

    if (minVersion(8, 0, 0)) {
      if (isMariaDBServer() || minVersion(8, 4, 0)) {
        stmt.execute(
            "CREATE USER 'StatementErrorUser'"
                + getHostSuffix()
                + " IDENTIFIED BY"
                + " 'MySup8%rPassw@ord'");
      } else {
        stmt.execute(
            "CREATE USER 'StatementErrorUser'"
                + getHostSuffix()
                + " IDENTIFIED WITH"
                + " mysql_native_password BY 'MySup8%rPassw@ord'");
      }
      stmt.execute("GRANT ALL ON *.* TO 'StatementErrorUser'" + getHostSuffix());
    } else {
      stmt.execute("CREATE USER 'StatementErrorUser'" + getHostSuffix());
      stmt.execute(
          "GRANT ALL ON *.* TO 'StatementErrorUser'"
              + getHostSuffix()
              + " IDENTIFIED BY"
              + " 'MySup8%rPassw@ord'");
    }
    stmt.execute("FLUSH PRIVILEGES");

    try {
      ConnectionPoolDataSource ds = new MariaDbDataSource(mDefUrl);
      PooledConnection pc = ds.getPooledConnection("StatementErrorUser", "MySup8%rPassw@ord");
      MyEventListener listener = new MyEventListener();
      pc.addStatementEventListener(listener);
      Connection connection = pc.getConnection();
      try (PreparedStatement ps = connection.prepareStatement("SELECT ?")) {
        ps.execute();
        fail("should never get there");
      } catch (Exception e) {
        assertTrue(listener.statementErrorOccurred);
      }
      assertTrue(listener.statementClosed);
      pc.close();
    } finally {
      stmt.execute("DROP USER IF EXISTS 'StatementErrorUser'" + getHostSuffix());
    }
  }

  public static class MyEventListener implements ConnectionEventListener, StatementEventListener {

    public SQLException sqlException;
    public boolean closed;
    public boolean connectionErrorOccurred;
    public boolean statementClosed;
    public boolean statementErrorOccurred;

    /** MyEventListener initialisation. */
    public MyEventListener() {
      sqlException = null;
      closed = false;
      connectionErrorOccurred = false;
    }

    public void connectionClosed(ConnectionEvent event) {
      sqlException = event.getSQLException();
      closed = true;
    }

    public void connectionErrorOccurred(ConnectionEvent event) {
      sqlException = event.getSQLException();
      connectionErrorOccurred = true;
    }

    public void statementClosed(StatementEvent event) {
      statementClosed = true;
    }

    public void statementErrorOccurred(StatementEvent event) {
      sqlException = event.getSQLException();
      statementErrorOccurred = true;
    }
  }
}
