// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.SingleStoreDataSource;
import java.sql.*;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import org.junit.jupiter.api.Test;

public class DataSourceTest extends Common {

  @Test
  public void basic() throws SQLException {
    SingleStoreDataSource ds = new SingleStoreDataSource(mDefUrl);
    testDs(ds);

    ds = new SingleStoreDataSource();
    ds.setUrl(mDefUrl);
    testDs(ds);

    SingleStoreDataSource ds2 = new SingleStoreDataSource();
    ds2.setUrl(mDefUrl);
    ds2.setPassword("ttt");
    ds2.setUser("ttt");
    assertThrows(SQLException.class, ds2::getConnection);
  }

  private void testDs(SingleStoreDataSource ds) throws SQLException {
    try (Connection con1 = ds.getConnection()) {
      try (Connection con2 = ds.getConnection()) {

        ResultSet rs1 = con1.createStatement().executeQuery("SELECT 1");
        ResultSet rs2 = con2.createStatement().executeQuery("SELECT 2");
        while (rs1.next()) {
          assertEquals(1, rs1.getInt(1));
        }
        while (rs2.next()) {
          assertEquals(2, rs2.getInt(1));
        }
      }
    }

    PooledConnection con1 = null;
    PooledConnection con2 = null;
    try {
      con1 = ds.getPooledConnection();
      con2 = ds.getPooledConnection();

      ResultSet rs1 = con1.getConnection().createStatement().executeQuery("SELECT 1");
      ResultSet rs2 = con2.getConnection().createStatement().executeQuery("SELECT 2");
      while (rs1.next()) {
        assertEquals(1, rs1.getInt(1));
      }
      while (rs2.next()) {
        assertEquals(2, rs2.getInt(1));
      }

    } finally {
      if (con1 != null) con1.close();
      if (con2 != null) con2.close();
    }

    XAConnection conx1 = null;
    XAConnection conx2 = null;
    try {
      conx1 = ds.getXAConnection();
      conx2 = ds.getXAConnection();

      ResultSet rs1 = conx1.getConnection().createStatement().executeQuery("SELECT 1");
      ResultSet rs2 = conx2.getConnection().createStatement().executeQuery("SELECT 2");
      while (rs1.next()) {
        assertEquals(1, rs1.getInt(1));
      }
      while (rs2.next()) {
        assertEquals(2, rs2.getInt(1));
      }

    } finally {
      if (conx1 != null) con1.close();
      if (conx2 != null) con2.close();
    }
  }

  @Test
  public void basic2() throws SQLException {
    SingleStoreDataSource ds = new SingleStoreDataSource();
    assertNull(ds.getUrl());
    assertNull(ds.getUser());
    assertEquals(30, ds.getLoginTimeout());
    DriverManager.setLoginTimeout(40);
    assertEquals(40, ds.getLoginTimeout());
    DriverManager.setLoginTimeout(0);
    ds.setLoginTimeout(50);
    assertEquals(50, ds.getLoginTimeout());

    assertThrows(SQLException.class, () -> ds.getConnection());
    assertThrows(SQLException.class, () -> ds.getConnection("user", "password"));
    assertThrows(SQLException.class, () -> ds.getPooledConnection());
    assertThrows(SQLException.class, () -> ds.getPooledConnection("user", "password"));
    assertThrows(SQLException.class, () -> ds.getXAConnection());
    assertThrows(SQLException.class, () -> ds.getXAConnection("user", "password"));

    ds.setUser("dd");
    assertEquals("dd", ds.getUser());

    ds.setPassword("pwd");
    assertThrows(SQLException.class, () -> ds.getConnection());
    assertThrows(SQLException.class, () -> ds.getPooledConnection());

    assertThrows(SQLException.class, () -> ds.setUrl("jdbc:wrong://d"));

    ds.setUrl("jdbc:singlestore://myhost:5500/db?someOption=val");
    assertEquals(
        "jdbc:singlestore://myhost:5500/db?user=dd&password=***&someOption=val&connectTimeout=50000",
        ds.getUrl());
  }

  @Test
  public void switchUser() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP USER IF EXISTS 'dsUser'");
    stmt.execute("CREATE USER 'dsUser'@'%' IDENTIFIED BY 'MySup8%rPassw@ord'");
    stmt.execute("GRANT SELECT ON " + sharedConn.getCatalog() + ".* TO 'dsUser'@'%'");
    stmt.execute("FLUSH PRIVILEGES");

    DataSource ds = new SingleStoreDataSource(mDefUrl);
    try (Connection con1 = ds.getConnection()) {
      try (Connection con2 = ds.getConnection("dsUser", "MySup8%rPassw@ord")) {
        ResultSet rs1 = con1.createStatement().executeQuery("SELECT 1");
        ResultSet rs2 = con2.createStatement().executeQuery("SELECT 2");
        while (rs1.next()) {
          assertEquals(1, rs1.getInt(1));
        }
        while (rs2.next()) {
          assertEquals(2, rs2.getInt(1));
        }
      }
    } finally {
      stmt.execute("DROP USER IF EXISTS 'dsUser'");
    }

    if (haveSsl()) {
      try (Connection con = createCon("sslMode=trust")) {
        con.createStatement().execute("SELECT 1");
      }
    }
  }

  @Test
  public void exceptions() throws SQLException {
    DataSource ds = new SingleStoreDataSource(mDefUrl);
    ds.unwrap(javax.sql.DataSource.class);
    ds.unwrap(SingleStoreDataSource.class);
    Common.assertThrowsContains(
        SQLException.class,
        () -> ds.unwrap(String.class),
        "Datasource is not a wrapper for java.lang.String");

    assertTrue(ds.isWrapperFor(javax.sql.DataSource.class));
    assertTrue(ds.isWrapperFor(SingleStoreDataSource.class));
    assertFalse(ds.isWrapperFor(String.class));
    Common.assertThrowsContains(
        SQLException.class,
        () -> new SingleStoreDataSource("jdbc:wrongUrl"),
        "Wrong SingleStoreDB url: jdbc:wrongUrl");
    assertNull(ds.getLogWriter());
    assertNull(ds.getParentLogger());
    ds.setLogWriter(null);

    assertEquals(30, ds.getLoginTimeout());
    ds.setLoginTimeout(60);
    assertEquals(60, ds.getLoginTimeout());
  }
}
