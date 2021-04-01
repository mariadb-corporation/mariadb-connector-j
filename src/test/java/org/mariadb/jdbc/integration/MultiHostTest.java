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

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.*;
import org.mariadb.jdbc.integration.tools.TcpProxy;

public class MultiHostTest extends Common {

  @Test
  public void failoverReadonlyToMaster() throws Exception {
    Assumptions.assumeTrue(
            !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    try (Connection con =
        createProxyConKeep("assureReadOnly=true&waitReconnectTimeout=300&deniedListTimeout=300")) {
      Statement stmt = con.createStatement();
      stmt.execute("START TRANSACTION");
      stmt.execute("SET @con=1");
      con.setReadOnly(true);
      final Statement stmt2 = con.createStatement();
      stmt2.execute("SET @con=2");
      proxy.restart(250);
      ResultSet rs = stmt2.executeQuery("SELECT @con");
      rs.next();
      assertEquals("1", rs.getString(1));
      Thread.sleep(500);
      rs = con.createStatement().executeQuery("SELECT @con");
      rs.next();
      assertNull(rs.getString(1));
    }
  }

  @Test
  public void syncState() throws Exception {
    Assumptions.assumeTrue(
            !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    try (Connection con = createProxyConKeep("assureReadOnly=true")) {
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
    Assumptions.assumeTrue(
            !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));

    String url = mDefUrl.replaceAll("jdbc:mariadb:", "jdbc:mariadb:replication:");
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
  public void masterFailover() throws Exception {
    Assumptions.assumeTrue(
            !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));

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
    url = url.replaceAll("jdbc:mariadb:", "jdbc:mariadb:sequential:");
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    try (Connection con =
        (Connection)
            DriverManager.getConnection(
                url
                    + "waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=4&connectTimeout=500")) {
      Statement stmt = con.createStatement();
      stmt.execute("SET @con=1");
      proxy.restart(50);

      ResultSet rs = stmt.executeQuery("SELECT @con");
      rs.next();
      assertNull(rs.getString(1));
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

      ResultSet rs = stmt.executeQuery("SELECT * from testReplay");
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
  public void masterReplicationFailover() throws Exception {
    Assumptions.assumeTrue(
            !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));

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
    url = url.replaceAll("jdbc:mariadb:", "jdbc:mariadb:replication:");
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    try (Connection con =
        (Connection)
            DriverManager.getConnection(
                url
                    + "waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=4&connectTimeout=500")) {
      Statement stmt = con.createStatement();
      stmt.execute("SET @con=1");
      con.setReadOnly(true);
      con.isValid(1);
      proxy.restart(50);
      Thread.sleep(20);
      con.setReadOnly(false);

      ResultSet rs = stmt.executeQuery("SELECT @con");
      rs.next();
      assertNull(rs.getString(1));
    }

    // never reconnect
    try (Connection con =
        (Connection)
            DriverManager.getConnection(
                url
                    + "waitReconnectTimeout=300&deniedListTimeout=300&retriesAllDown=4&connectTimeout=500")) {
      Statement stmt = con.createStatement();
      stmt.execute("SET @con=1");
      con.setReadOnly(true);
      con.isValid(1);
      proxy.stop();
      Thread.sleep(20);
      con.setReadOnly(false);
      assertFalse(con.isValid(1));
    }
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
    url = url.replaceAll("jdbc:mariadb:", "jdbc:mariadb:replication:");
    if (conf.sslMode() == SslMode.VERIFY_FULL) {
      url = url.replaceAll("sslMode=verify-full", "sslMode=verify-ca");
    }

    return (Connection) DriverManager.getConnection(url + opts);
  }
}
