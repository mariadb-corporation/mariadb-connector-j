// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.export.SslMode;
import com.singlestore.jdbc.integration.tools.TcpProxy;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class MultiHostTest extends Common {

  @Test
  public void failoverReadonlyToMaster() throws Exception {
    try (Connection con = createProxyConKeep("waitReconnectTimeout=300&deniedListTimeout=300")) {
      long primaryThreadId = con.getThreadId();
      con.setReadOnly(true);
      long replicaThreadId = con.getThreadId();
      assertTrue(primaryThreadId != replicaThreadId);

      con.setReadOnly(false);
      assertEquals(primaryThreadId, con.getThreadId());
      con.setReadOnly(true);
      assertEquals(replicaThreadId, con.getThreadId());
      proxy.restart(250);

      con.isValid(1);
      assertEquals(primaryThreadId, con.getThreadId());
    }
  }

  @Test
  public void syncState() throws Exception {
    try (Connection con = createProxyConKeep("")) {
      Statement stmt = con.createStatement();
      stmt.execute("CREATE DATABASE IF NOT EXISTS sync");
      con.setCatalog("sync");
      con.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);
      con.setReadOnly(true);
      assertEquals("sync", con.getCatalog());
      assertEquals(java.sql.Connection.TRANSACTION_SERIALIZABLE, con.getTransactionIsolation());
      con.setReadOnly(true);
      con.setReadOnly(false);
      assertEquals(java.sql.Connection.TRANSACTION_SERIALIZABLE, con.getTransactionIsolation());
      con.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
      con.setReadOnly(true);
      assertEquals(java.sql.Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation());
      con.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED);
      con.setReadOnly(false);
      assertEquals(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED, con.getTransactionIsolation());
      con.setTransactionIsolation(java.sql.Connection.TRANSACTION_REPEATABLE_READ);
      con.setReadOnly(true);
      assertEquals(java.sql.Connection.TRANSACTION_REPEATABLE_READ, con.getTransactionIsolation());
    } finally {
      sharedConn.createStatement().execute("DROP DATABASE IF EXISTS sync");
    }
  }

  @Test
  public void replicaNotSet() throws Exception {
    String url = mDefUrl.replaceAll("jdbc:singlestore:", "jdbc:singlestore:replication:");
    try (java.sql.Connection con = DriverManager.getConnection(url + "&waitReconnectTimeout=20")) {
      con.isValid(1);
      con.setReadOnly(true);
      con.isValid(1);
      // force reconnection try
      Thread.sleep(50);
      con.isValid(1);
    }
  }

  @Test
  public void closedConnectionMulti() throws Exception {

    Configuration conf = Configuration.parse(mDefUrl);
    HostAddress hostAddress = conf.addresses().get(0);
    String url =
        mDefUrl.replaceAll(
            "//([^/]*)/",
            String.format(
                "//address=(host=localhost)(port=9999)(type=master),address=(host=%s)(port=%s)(type=master)/",
                hostAddress.host, hostAddress.port));
    url = url.replaceAll("jdbc:singlestore:", "jdbc:singlestore:sequential:");
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    Connection con =
        (Connection)
            DriverManager.getConnection(
                url
                    + "&waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=4&connectTimeout=500&useServerPrepStmts&cachePrepStmts=false");
    testClosedConn(con);

    url =
        mDefUrl.replaceAll(
            "//([^/]*)/",
            String.format(
                "//%s:%s,%s,%s/",
                hostAddress.host, hostAddress.port, hostAddress.host, hostAddress.port));
    url = url.replaceAll("jdbc:singlestore:", "jdbc:singlestore:replication:");
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    con =
        (Connection)
            DriverManager.getConnection(
                url
                    + "&waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=4&connectTimeout=500&useServerPrepStmts&cachePrepStmts=false");
    testClosedConn(con);
  }

  private void testClosedConn(Connection con) throws SQLException {
    PreparedStatement prep = con.prepareStatement("SELECT ?");
    PreparedStatement prep2 = con.prepareStatement("SELECT 1, ?");
    prep2.setString(1, "1");
    prep2.execute();
    Statement stmt = con.createStatement();
    stmt.setFetchSize(1);
    ResultSet rs = stmt.executeQuery("SELECT * FROM sequence_1_to_10");
    rs.next();

    con.close();

    prep.setString(1, "1");
    assertThrowsContains(SQLException.class, () -> prep.execute(), "Connection is closed");
    assertThrowsContains(SQLException.class, () -> prep2.execute(), "Connection is closed");
    assertThrowsContains(SQLException.class, () -> prep2.close(), "Connection is closed");
    con.close();
    assertThrowsContains(
        SQLException.class,
        () -> con.abort(null),
        "Cannot abort the connection: null executor passed");
    assertNotNull(con.getClient().getHostAddress());
    assertThrowsContains(
        SQLException.class,
        () -> con.getClient().readStreamingResults(null, 0, 0, 0, 0, true),
        "Connection is closed");
    con.getClient().reset();
  }

  @Test
  @Disabled
  public void masterFailover() throws Exception {

    Configuration conf = Configuration.parse(mDefUrl);
    HostAddress hostAddress = conf.addresses().get(0);
    try {
      proxy = new TcpProxy(hostAddress.host, hostAddress.port);
    } catch (IOException i) {
      throw new SQLException("proxy error", i);
    }

    String url =
        mDefUrl.replaceAll(
            "//([^/]*)/",
            String.format(
                "//address=(host=localhost)(port=9999)(type=master),address=(host=localhost)(port=%s)(type=master),address=(host=%s)(port=%s)(type=master)/",
                proxy.getLocalPort(), hostAddress.host, hostAddress.port));
    url = url.replaceAll("jdbc:singlestore:", "jdbc:singlestore:sequential:");
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    if (minVersion(7, 3, 0)) {
      try (Connection con =
          (Connection)
              DriverManager.getConnection(
                  url
                      + "waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=4&connectTimeout=500")) {
        Statement stmt = con.createStatement();
        stmt.execute("SELECT 1 INTO @con");
        proxy.restart(50);

        Common.assertThrowsContains(
            SQLException.class,
            () -> stmt.executeQuery("SELECT @con"),
            "Unknown user-defined variable");
      }
    }
    Thread.sleep(50);

    // with transaction replay
    try (Connection con =
        (Connection)
            DriverManager.getConnection(
                url
                    + "transactionReplay=true&waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=4&connectTimeout=500")) {
      Statement stmt = con.createStatement();
      stmt.execute("DROP TABLE IF EXISTS testReplay");
      stmt.execute("CREATE TABLE testReplay(id INT)");
      stmt.execute("INSERT INTO testReplay VALUE (1)");
      stmt.execute("START TRANSACTION");
      stmt.execute("INSERT INTO testReplay VALUE (2)");
      try (PreparedStatement prep = con.prepareStatement("INSERT INTO testReplay VALUE (?)")) {
        prep.setInt(1, 3);
        prep.execute();
      }

      try (PreparedStatement prep = con.prepareStatement("INSERT INTO testReplay VALUE (?)")) {
        prep.setInt(1, 4);
        prep.execute();
        proxy.restart(50);
        prep.setInt(1, 5);
        prep.execute();
      }

      ResultSet rs = stmt.executeQuery("SELECT * from testReplay ORDER BY id");
      rs.next();
      assertEquals(1, rs.getInt(1));
      rs.next();
      assertEquals(2, rs.getInt(1));
      rs.next();
      assertEquals(3, rs.getInt(1));
      rs.next();
      assertEquals(4, rs.getInt(1));
      rs.next();
      assertEquals(5, rs.getInt(1));
      assertFalse(rs.next());
      stmt.execute("DROP TABLE IF EXISTS testReplay");
    }
  }

  @Test
  public void masterStreamingFailover() throws Exception {
    Configuration conf = Configuration.parse(mDefUrl);
    HostAddress hostAddress = conf.addresses().get(0);
    try {
      proxy = new TcpProxy(hostAddress.host, hostAddress.port);
    } catch (IOException i) {
      throw new SQLException("proxy error", i);
    }

    String url =
        mDefUrl.replaceAll(
            "//([^/]*)/",
            String.format(
                "//address=(host=localhost)(port=%s)(type=master)/", proxy.getLocalPort()));
    url = url.replaceAll("jdbc:singlestore:", "jdbc:singlestore:sequential:");
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    Connection con =
        (Connection)
            DriverManager.getConnection(
                url
                    + "allowMultiQueries&transactionReplay=true&waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=40&connectTimeout=500&useReadAheadInput=false");
    long threadId = con.getThreadId();
    Statement stmt = con.createStatement();
    stmt.setFetchSize(2);
    ensureRange(stmt);
    ensureLargeRange(stmt, 50000);
    ResultSet rs =
        stmt.executeQuery(
            "SELECT * FROM range_1_100 ORDER BY n; SELECT * FROM large_range ORDER BY n;");
    rs.setFetchSize(0);
    rs.next();
    proxy.restart(50);
    Statement stmt2 = con.createStatement();
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt2.executeQuery("SHOW USERS"),
        "Socket error during result streaming");
    assertNotEquals(threadId, con.getThreadId());

    // additional small test
    assertEquals(0, con.getNetworkTimeout());
    con.setNetworkTimeout(Runnable::run, 10);
    assertEquals(10, con.getNetworkTimeout());

    con.setReadOnly(true);
    con.close();
    Common.assertThrowsContains(
        SQLNonTransientConnectionException.class,
        () -> con.setReadOnly(false),
        "Connection is closed");
    con.abort(Runnable::run); // no-op

    Connection con2 =
        (Connection)
            DriverManager.getConnection(
                url
                    + "allowMultiQueries&transactionReplay=true&waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=40&connectTimeout=500&useReadAheadInput=false");
    con2.abort(Runnable::run);
  }

  @Test
  public void masterReplicationFailover() throws Exception {
    Configuration conf = Configuration.parse(mDefUrl);
    HostAddress hostAddress = conf.addresses().get(0);
    try {
      proxy = new TcpProxy(hostAddress.host, hostAddress.port);
    } catch (IOException i) {
      throw new SQLException("proxy error", i);
    }

    String url =
        mDefUrl.replaceAll(
            "//([^/]*)/",
            String.format(
                "//localhost:%s,%s:%s/", proxy.getLocalPort(), hostAddress.host, hostAddress.port));
    url = url.replaceAll("jdbc:singlestore:", "jdbc:singlestore:replication:");
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    if (minVersion(7, 3, 0)) {
      try (Connection con =
          (Connection)
              DriverManager.getConnection(
                  url
                      + "&waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=4&connectTimeout=500")) {
        Statement stmt = con.createStatement();
        stmt.execute("SELECT 1 INTO @con");
        con.setReadOnly(true);
        con.isValid(1);
        proxy.restart(50);
        Thread.sleep(20);
        con.setReadOnly(false);

        assertThrowsContains(
            SQLTransientConnectionException.class,
            () -> stmt.executeQuery("SELECT @con"),
            "Driver has reconnect connection after a communications link failure with");
      }
    }

    // never reconnect
    if (minVersion(7, 3, 0)) {
      try (Connection con =
          (Connection)
              DriverManager.getConnection(
                  url
                      + "waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=4&connectTimeout=500")) {
        Statement stmt = con.createStatement();
        stmt.execute("SELECT 1 INTO @con");
        con.setReadOnly(true);
        con.isValid(1);
        proxy.stop();
        Thread.sleep(20);
        con.setReadOnly(false);
        assertFalse(con.isValid(1));
        assertThrows(SQLException.class, () -> stmt.execute("SELECT 1"));
      }
    }
  }

  @Test
  public void masterReplicationStreamingFailover() throws Exception {
    Configuration conf = Configuration.parse(mDefUrl);
    HostAddress hostAddress = conf.addresses().get(0);
    try {
      proxy = new TcpProxy(hostAddress.host, hostAddress.port);
    } catch (IOException i) {
      throw new SQLException("proxy error", i);
    }

    String url =
        mDefUrl.replaceAll(
            "//([^/]*)/",
            String.format(
                "//address=(host=localhost)(port=%s)(type=primary),address=(host=%s)(port=%s)(type=replica)/",
                proxy.getLocalPort(), hostAddress.host, hostAddress.port));
    url = url.replaceAll("jdbc:singlestore:", "jdbc:singlestore:replication:");
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    Connection con =
        (Connection)
            DriverManager.getConnection(
                url
                    + "allowMultiQueries&transactionReplay=true&waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=40&connectTimeout=500&useReadAheadInput=false");
    long threadId = con.getThreadId();
    Statement stmt = con.createStatement();
    stmt.setFetchSize(2);
    ensureRange(stmt);
    ensureLargeRange(stmt, 50000);
    ResultSet rs =
        stmt.executeQuery(
            "SELECT * FROM range_1_100 ORDER BY n; SELECT * FROM large_range ORDER BY n");
    rs.setFetchSize(0);
    rs.next();
    assertEquals(1, rs.getInt(1));
    proxy.restart(50);
    Statement stmt2 = con.createStatement();
    Common.assertThrowsContains(
        SQLException.class,
        () -> stmt2.executeQuery("SHOW USERS"),
        "Socket error during result streaming");
    assertNotEquals(threadId, con.getThreadId());

    // additional small test
    assertEquals(0, con.getNetworkTimeout());
    con.setNetworkTimeout(Runnable::run, 10);
    assertEquals(10, con.getNetworkTimeout());

    con.setReadOnly(true);
    con.close();
    Common.assertThrowsContains(
        SQLNonTransientConnectionException.class,
        () -> con.setReadOnly(false),
        "Connection is closed");
    con.abort(Runnable::run); // no-op
    Connection con2 =
        (Connection)
            DriverManager.getConnection(
                url
                    + "allowMultiQueries&transactionReplay=true&waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=40&connectTimeout=500&useReadAheadInput=false");
    con2.abort(Runnable::run);
  }

  public Connection createProxyConKeep(String opts) throws SQLException {
    Configuration conf = Configuration.parse(mDefUrl);
    HostAddress hostAddress = conf.addresses().get(0);
    try {
      proxy = new TcpProxy(hostAddress.host, hostAddress.port);
    } catch (IOException i) {
      throw new SQLException("proxy error", i);
    }

    String url =
        mDefUrl.replaceAll(
            "//([^/]*)/",
            String.format(
                "//%s:%s,localhost:%s/", hostAddress.host, hostAddress.port, proxy.getLocalPort()));
    url = url.replaceAll("jdbc:singlestore:", "jdbc:singlestore:replication:");
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    return (Connection) DriverManager.getConnection(url + opts);
  }
}
