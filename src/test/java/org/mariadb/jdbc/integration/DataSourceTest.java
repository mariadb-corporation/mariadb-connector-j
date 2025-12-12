// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbDataSource;

public class DataSourceTest extends Common {

  @Test
  public void basic() throws SQLException {
    MariaDbDataSource ds = new MariaDbDataSource(mDefUrl);
    testDs(ds);

    ds = new MariaDbDataSource();
    ds.setUrl(mDefUrl);
    testDs(ds);

    MariaDbDataSource ds2 = new MariaDbDataSource();
    ds2.setUrl(mDefUrl);
    ds2.setPassword(System.getenv("someP@ssword"));
    ds2.setUser("ttt");
    assertThrows(SQLException.class, ds2::getConnection);
  }

  @Test
  public void basicTimeout() throws SQLException {
    MariaDbDataSource ds = new MariaDbDataSource(mDefUrl);
    ds.setLoginTimeout(0);
    testDs(ds);
    ds.setLoginTimeout(0);
    testDs(ds);
  }


  private void testDs(MariaDbDataSource ds) throws SQLException {
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
      try (Statement stmt = con1.getConnection().createStatement()) {
        ResultSet rs1 = stmt.executeQuery("SELECT 1");
        try (Statement stmt2 = con2.getConnection().createStatement()) {
          ResultSet rs2 = stmt2.executeQuery("SELECT 2");
          while (rs1.next()) {
            assertEquals(1, rs1.getInt(1));
          }
          while (rs2.next()) {
            assertEquals(2, rs2.getInt(1));
          }
        }
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

      try (Statement stmt = conx1.getConnection().createStatement()) {
        ResultSet rs1 = stmt.executeQuery("SELECT 1");
        try (Statement stmt2 = conx2.getConnection().createStatement()) {
          ResultSet rs2 = stmt2.executeQuery("SELECT 2");
          while (rs1.next()) {
            assertEquals(1, rs1.getInt(1));
          }
          while (rs2.next()) {
            assertEquals(2, rs2.getInt(1));
          }
        }
      }
    } finally {
      if (conx1 != null) con1.close();
      if (conx2 != null) con2.close();
    }
  }

  @Test
  public void basic2() throws SQLException {
    MariaDbDataSource ds = new MariaDbDataSource();
    assertNull(ds.getUrl());
    assertNull(ds.getUser());
    assertEquals(30, ds.getLoginTimeout());
    DriverManager.setLoginTimeout(40);
    assertEquals(40, ds.getLoginTimeout());
    DriverManager.setLoginTimeout(0);
    ds.setLoginTimeout(50);
    assertEquals(50, ds.getLoginTimeout());

    assertThrows(SQLException.class, ds::getConnection);
    assertThrows(SQLException.class, () -> ds.getConnection("user", "password"));
    assertThrows(SQLException.class, ds::getPooledConnection);
    assertThrows(SQLException.class, () -> ds.getPooledConnection("user", "password"));
    assertThrows(SQLException.class, ds::getXAConnection);
    assertThrows(SQLException.class, () -> ds.getXAConnection("user", "password"));

    ds.setUser("dd");
    assertEquals("dd", ds.getUser());

    ds.setPassword(System.getenv("someOtherP@ssword"));
    assertThrows(SQLException.class, ds::getConnection);
    assertThrows(SQLException.class, ds::getPooledConnection);

    assertThrows(SQLException.class, () -> ds.setUrl("jdbc:wrong://d"));

    ds.setUrl("jdbc:mariadb://myhost:5500/db?someOption=val");
    assertEquals(
        "jdbc:mariadb://myhost:5500/db?user=dd&someOption=val&connectTimeout=50000", ds.getUrl());
  }

  @Test
  public void switchUser() throws SQLException {
    Assumptions.assumeTrue(!isMaxscale());
    Statement stmt = sharedConn.createStatement();
    try {
      stmt.execute("DROP USER 'dsUser'" + getHostSuffix());
    } catch (SQLException e) {
      // eat
    }

    if (minVersion(8, 0, 0)) {
      if (isMariaDBServer() || minVersion(8, 4, 0)) {
        stmt.execute(
            "CREATE USER 'dsUser'" + getHostSuffix() + " IDENTIFIED BY 'MySup8%rPassw@ord'");
      } else {
        stmt.execute(
            "CREATE USER 'dsUser'"
                + getHostSuffix()
                + " IDENTIFIED WITH mysql_native_password BY"
                + " 'MySup8%rPassw@ord'");
      }
      stmt.execute("GRANT ALL ON *.* TO 'dsUser'" + getHostSuffix());
    } else {
      stmt.execute("CREATE USER 'dsUser'" + getHostSuffix());
      stmt.execute(
          "GRANT SELECT ON "
              + sharedConn.getCatalog()
              + ".* TO 'dsUser'"
              + getHostSuffix()
              + " IDENTIFIED BY 'MySup8%rPassw@ord'");
    }
    stmt.execute("FLUSH PRIVILEGES");

    DataSource ds = new MariaDbDataSource(mDefUrl + "&allowPublicKeyRetrieval");
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
      try {
        stmt.execute("DROP USER 'dsUser'" + getHostSuffix());
      } catch (SQLException e) {
        // eat
      }
    }

    // mysql has issue when creating new user with native password
    if (haveSsl() && !isMariaDBServer() && minVersion(8, 0, 0)) {
      try (Connection con = createCon("sslMode=trust")) {
        con.createStatement().execute("DO 1");
      }
    }
  }

  @Test
  public void exceptions() throws SQLException {
    DataSource ds = new MariaDbDataSource(mDefUrl);
    ds.unwrap(javax.sql.DataSource.class);
    ds.unwrap(MariaDbDataSource.class);
    Common.assertThrowsContains(
        SQLException.class,
        () -> ds.unwrap(String.class),
        "Datasource is not a wrapper for java.lang.String");

    assertTrue(ds.isWrapperFor(javax.sql.DataSource.class));
    assertTrue(ds.isWrapperFor(MariaDbDataSource.class));
    assertFalse(ds.isWrapperFor(String.class));
    Common.assertThrowsContains(
        SQLException.class,
        () -> new MariaDbDataSource("jdbc:wrongUrl"),
        "Wrong mariaDB url: jdbc:wrongUrl");
    assertNull(ds.getLogWriter());
    assertNull(ds.getParentLogger());
    ds.setLogWriter(null);

    assertEquals(30, ds.getLoginTimeout());
    ds.setLoginTimeout(60);
    assertEquals(60, ds.getLoginTimeout());
  }

  @Test
  public void ensureConnectionClose() throws Exception {
    MariaDbDataSource datasource = new MariaDbDataSource(mDefUrl);

    Connection c = datasource.getConnection();
    assertFalse(c.isClosed());
    c.close();
    assertTrue(c.isClosed());

    PooledConnection pc = datasource.getPooledConnection();
    assertFalse(pc.getConnection().isClosed());
    pc.getConnection().close();
    assertFalse(pc.getConnection().isClosed());
    pc.close();
    assertTrue(pc.getConnection().isClosed());

    XAConnection xac = datasource.getXAConnection();
    assertFalse(xac.getConnection().isClosed());
    xac.getConnection().close();
    assertFalse(xac.getConnection().isClosed());
    xac.close();
    assertTrue(xac.getConnection().isClosed());
  }
}
