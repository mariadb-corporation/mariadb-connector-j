// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.Collections;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.NonRegisteringDriver;

public class DriverTest extends NonRegisteringDriverTest {

  @Override
  protected Driver newDriver() {
    return new org.mariadb.jdbc.Driver();
  }

  @Test
  public void registersInDriverManager() throws ClassNotFoundException {
    // the only behaviour that sets this driver apart from its parent class
    Class.forName("org.mariadb.jdbc.Driver");
    boolean registered = false;
    for (Driver driver : Collections.list(DriverManager.getDrivers())) {
      if (driver.getClass() == org.mariadb.jdbc.Driver.class) {
        registered = true;
      }
    }
    assertTrue(registered, "org.mariadb.jdbc.Driver must register itself in DriverManager");
  }

  @Test
  public void staticHelpersInheritedFromParent() throws SQLException {
    // the static helpers live on NonRegisteringDriver : callers naming Driver must still compile
    // and get the same answers
    assertEquals(
        NonRegisteringDriver.enquoteLiteral("a'b"), org.mariadb.jdbc.Driver.enquoteLiteral("a'b"));
    assertEquals(
        NonRegisteringDriver.enquoteIdentifier("a`b", true),
        org.mariadb.jdbc.Driver.enquoteIdentifier("a`b", true));
    assertEquals(
        NonRegisteringDriver.isSimpleIdentifier("simple"),
        org.mariadb.jdbc.Driver.isSimpleIdentifier("simple"));
  }
}
