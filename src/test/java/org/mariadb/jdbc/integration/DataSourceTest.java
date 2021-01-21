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

import java.io.*;
import java.sql.*;
import javax.sql.DataSource;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.MariaDbDataSource;

public class DataSourceTest extends Common {

  @Test
  public void basic() throws SQLException {
    DataSource ds = new MariaDbDataSource(mDefUrl);
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
  }

  @Test
  public void switchUser() throws SQLException {
    Assumptions.assumeTrue(System.getenv("MAXSCALE_TEST_DISABLE") == null);
    Statement stmt = sharedConn.createStatement();
    if (minVersion(8, 0, 0)) {
      stmt.execute("CREATE USER 'dsUser'@'%' IDENTIFIED BY 'MySup8%rPassw@ord'");
      stmt.execute("GRANT SELECT ON *.* TO 'dsUser'@'%'");
    } else {
      stmt.execute("CREATE USER 'dsUser'@'%'");
      stmt.execute("GRANT SELECT ON *.* TO 'dsUser'@'%' IDENTIFIED BY 'MySup8%rPassw@ord'");
    }
    stmt.execute("FLUSH PRIVILEGES");

    DataSource ds = new MariaDbDataSource(mDefUrl);
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
      stmt.execute("DROP USER 'dsUser'");
    }
  }

  @Test
  public void exceptions() throws SQLException {
    DataSource ds = new MariaDbDataSource(mDefUrl);
    ds.unwrap(javax.sql.DataSource.class);
    ds.unwrap(MariaDbDataSource.class);
    assertThrowsContains(
        SQLException.class,
        () -> ds.unwrap(String.class),
        "Datasource is not a wrapper for java.lang.String");

    assertTrue(ds.isWrapperFor(javax.sql.DataSource.class));
    assertTrue(ds.isWrapperFor(MariaDbDataSource.class));
    assertFalse(ds.isWrapperFor(String.class));
    assertThrowsContains(
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
}
