// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.SingleStoreDataSource;
import com.singlestore.jdbc.SingleStorePoolDataSource;
import com.singlestore.jdbc.export.SslMode;
import com.singlestore.jdbc.integration.tools.TcpProxy;
import com.singlestore.jdbc.pool.InternalPoolConnection;
import com.singlestore.jdbc.pool.Pool;
import com.singlestore.jdbc.pool.Pools;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

public class PooledConnectionTest extends Common {

  @Test
  public void testPooledConnectionClosed() throws Exception {
    ConnectionPoolDataSource ds = new SingleStoreDataSource(mDefUrl);
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
    try (SingleStorePoolDataSource ds =
        new SingleStorePoolDataSource(
            mDefUrl + "&sessionVariables=wait_timeout=1&maxIdleTime=2&testMaxRemovalDelay=2")) {
      Thread.sleep(4000);
      PooledConnection pc = ds.getPooledConnection();
      pc.getConnection().isValid(1);
      pc.close();
    }
  }

  @Test
  public void testPoolInvalidIdleTime() {
    Common.assertThrowsContains(
        SQLException.class,
        () ->
            new SingleStorePoolDataSource(
                mDefUrl + "&sessionVariables=wait_timeout=1&maxIdleTime=1&testMaxRemovalDelay=2"),
        "Wrong argument value '1' for maxIdleTime, must be >= 2");
  }

  @Test
  public void testPoolWaitWithValidation() throws Exception {
    try (SingleStorePoolDataSource ds =
        new SingleStorePoolDataSource(mDefUrl + "&poolValidMinDelay=1")) {
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

    try (SingleStorePoolDataSource ds =
        new SingleStorePoolDataSource(
            url + "poolValidMinDelay=1&connectTimeout=50&maxPoolSize=1")) {

      PooledConnection pc = ds.getPooledConnection();
      pc.getConnection().isValid(1);
      pc.close();
      Thread.sleep(200);
      proxy.stop();
      Common.assertThrowsContains(
          SQLException.class,
          () -> ds.getPooledConnection(),
          "No connection available within the specified time");
    }
  }

  @Test
  public void testPoolKillConnection() throws Exception {
    File tempFile = File.createTempFile("log", ".tmp");

    Logger logger = (Logger) LoggerFactory.getLogger("com.singlestore.jdbc");
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

    try (SingleStorePoolDataSource ds = new SingleStorePoolDataSource(mDefUrl + "&maxPoolSize=1")) {
      InternalPoolConnection pc = ds.getPooledConnection();
      com.singlestore.jdbc.Connection conn = pc.getConnection();
      long threadId = conn.getThreadId();
      try {
        conn.createStatement().execute("KILL " + threadId);
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Socket error"));
      }
      pc.close();
      pc = ds.getPooledConnection();
      conn = pc.getConnection();
      pc.close();
    } finally {

      String contents = new String(Files.readAllBytes(Paths.get(tempFile.getPath())));
      assertTrue(
          contents.contains(
              "removed from pool SingleStore-pool due to having throw a Connection exception (total:0, active:0, pending:0)"));
      // if connection is failed connection pool sends add new connection request
      // asynchronously
      // sometimes it can have 2 connections created instead of 1
      assertTrue(
          contents.contains("closing pool SingleStore-pool (total:1, active:0, pending:0)")
              || contents.contains("closing pool SingleStore-pool (total:2, active:1, pending:0)"));
      logger.setLevel(initialLevel);
      logger.detachAppender(fa);
    }
  }

  private int countQueries(String hostPort, String database) throws Exception {
    Connection con = getConnection(hostPort, database);
    ResultSet rs =
        con.createStatement().executeQuery("SHOW STATUS extended LIKE 'Successful_write_queries'");
    int sum = 0;
    while (rs.next()) {
      sum += Integer.parseInt(rs.getString(2));
    }
    return sum;
  }

  private Connection getConnection(String hostPort, String database) throws Exception {
    String url =
        String.format(
            "jdbc:singlestore://%s/%s?user=%s&password=%s&restrictedAuth=none&maxPoolSize=10",
            hostPort, database, user, password);
    return DriverManager.getConnection(url);
  }

  @Test
  public void testPooledConnectionLoadBalance() throws Exception {
    String host = "localhost", portMaster = "5506", portChild = "5508", database = "test";
    String url =
        String.format(
            "jdbc:singlestore:loadbalance//%s:%s,%s:%s/%s?user=%s&password=%s&restrictedAuth=none&maxPoolSize=10",
            host, portMaster, host, portChild, database, "root", password);
    ConnectionPoolDataSource ds = new SingleStorePoolDataSource(url);

    try (Connection conn = getConnection(host + ":" + portMaster, database)) {
      conn.createStatement().execute("DROP TABLE IF EXISTS loadbalance");
      conn.createStatement().execute("CREATE TABLE loadbalance (id int)");
    }

    int host1QueriesBefore = countQueries(host + ":" + portMaster, database);
    int host2QueriesBefore = countQueries(host + ":" + portChild, database);

    Connection[] cons = new Connection[10];
    for (int i = 0; i < 10; i++) {
      cons[i] = ds.getPooledConnection().getConnection();
      PreparedStatement preparedStatement =
          cons[i].prepareStatement("INSERT INTO loadbalance VALUES (?)");
      preparedStatement.setInt(1, i);
      preparedStatement.execute();
    }
    for (int i = 0; i < 10; i++) {
      cons[i].close();
    }

    int host1QueriesAfter = countQueries(host + ":" + portMaster, database);
    int host2QueriesAfter = countQueries(host + ":" + portChild, database);

    try (Connection conn = getConnection(host + ":" + portMaster, database)) {
      conn.createStatement().execute("DROP TABLE IF EXISTS loadbalance");
    }

    assertTrue(host1QueriesAfter > host1QueriesBefore);
    assertTrue(host2QueriesAfter > host2QueriesBefore);
  }

  @Test
  public void testPooledConnectionException() throws Exception {
    ConnectionPoolDataSource ds = new SingleStoreDataSource(mDefUrl);
    PooledConnection pc = null;
    try {
      pc = ds.getPooledConnection();
      MyEventListener listener = new MyEventListener();
      pc.addConnectionEventListener(listener);
      Connection connection = pc.getConnection();

      /* Ask server to abort the connection */
      try {
        ResultSet rs = connection.createStatement().executeQuery("SELECT CONNECTION_ID()");
        assertTrue(rs.next());
        Integer connectionId = rs.getInt(1);
        connection.createStatement().execute(String.format("KILL %s", connectionId));
      } catch (Exception e) {
        /* exception is expected here, server sends query aborted */
      }

      /* Try to read after server side closed the connection */
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
      InternalPoolConnection pooledConnection = pool.getPoolConnection();
      com.singlestore.jdbc.Connection con = pooledConnection.getConnection();
      con.setAutoCommit(false);
      con.createStatement().execute("START TRANSACTION ");

      Connection con2 = pool.getPoolConnection().getConnection();
      con2.createStatement().execute("KILL " + con.getThreadId());
      con2.close();
      Thread.sleep(10);
      assertThrows(SQLException.class, () -> con.commit());
      pooledConnection.close();
    }
  }

  @Test
  public void testPooledConnectionStatementError() throws Exception {
    Statement stmt = sharedConn.createStatement();

    stmt.execute("DROP USER IF EXISTS 'dsUser'");
    stmt.execute("CREATE USER 'dsUser'@'%' IDENTIFIED BY 'MySup8%rPassw@ord'");
    stmt.execute("GRANT SELECT ON " + sharedConn.getCatalog() + ".* TO 'dsUser'@'%'");
    stmt.execute("FLUSH PRIVILEGES");

    ConnectionPoolDataSource ds = new SingleStoreDataSource(mDefUrl);
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
    assertTrue(pc.getConnection().isValid(1));
    pc.close();
  }

  @Test
  public void testPooledConnectionDBError() throws Exception {
    Connection con =
        DriverManager.getConnection(
            String.format(
                "jdbc:singlestore://localhost:%s/?user=%s&password=%s&pool=True&maxPoolSize=1",
                port, user, password));

    Statement stmt = con.createStatement();
    try {
      stmt.execute("SELECT * FROM t WHERE 1=0");
    } catch (SQLException ignored) {
    }

    assertFalse(con.isValid(1));
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
