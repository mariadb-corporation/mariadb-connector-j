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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.*;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.*;
import org.mariadb.jdbc.integration.tools.TcpProxy;
import org.mariadb.jdbc.pool.InternalPoolConnection;
import org.slf4j.LoggerFactory;

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

    String url = mDefUrl.replaceAll("//([^/]*)/", "//localhost:" + proxy.getLocalPort() + "/");
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    try (MariaDbPoolDataSource ds =
        new MariaDbPoolDataSource(url + "poolValidMinDelay=1&connectTimeout=10&maxPoolSize=1")) {

      PooledConnection pc = ds.getPooledConnection();
      pc.getConnection().isValid(1);
      pc.close();
      Thread.sleep(200);
      proxy.stop();
      assertThrowsContains(
          SQLException.class,
          () -> ds.getPooledConnection(),
          "No connection available within the specified time");
    }
  }

  @Test
  public void testPoolKillConnection() throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));

    File tempFile = File.createTempFile("log", ".tmp");

    Logger logger = (Logger) LoggerFactory.getLogger("org.mariadb.jdbc");
    Level initialLevel = logger.getLevel();
    logger.setLevel(Level.TRACE);
    logger.setAdditive(false);
    logger.detachAndStopAllAppenders();

    LoggerContext context = new LoggerContext();
    FileAppender<ILoggingEvent> fa = new FileAppender<ILoggingEvent>();
    fa.setName("FILE");
    fa.setImmediateFlush(true);
    PatternLayoutEncoder pa = new PatternLayoutEncoder();
    pa.setPattern("%r %5p %c [%t] - %m%n");
    pa.setContext(context);
    pa.start();
    fa.setEncoder(pa);

    fa.setFile(tempFile.getPath());
    fa.setAppend(true);
    fa.setContext(context);
    fa.start();

    logger.addAppender(fa);

    try (MariaDbPoolDataSource ds =
        new MariaDbPoolDataSource(mDefUrl + "&maxPoolSize=1&allowPublicKeyRetrieval")) {
      InternalPoolConnection pc = ds.getPooledConnection();
      org.mariadb.jdbc.Connection conn = pc.getConnection();
      long threadId = conn.getThreadId();
      try {
        conn.createStatement().execute("KILL " + threadId);
      } catch (SQLException e) {
        // eat "Connection was killed" message
      }
      pc.close();
      pc = ds.getPooledConnection();
      conn = pc.getConnection();
      assertNotEquals(threadId, conn.getThreadId());
      pc.close();
    } finally {

      String contents = new String(Files.readAllBytes(Paths.get(tempFile.getPath())));
      assertTrue(
          contents.contains(
              "removed from pool MariaDB-pool due to having throw a Connection exception (total:1, active:1, pending:0)"));
      assertTrue(
          contents.contains("connection removed from pool MariaDB-pool due to error during reset"));
      assertTrue(contents.contains("closing pool MariaDB-pool (total:1, active:0, pending:0)"));
      logger.setLevel(initialLevel);
      logger.detachAppender(fa);
    }
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
