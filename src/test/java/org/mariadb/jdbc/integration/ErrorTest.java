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

import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;

public class ErrorTest extends Common {

  @BeforeAll
  public static void begin() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE deadlock(a int primary key)");
    stmt.execute("FLUSH TABLES");
  }

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS deadlock");
  }

  @Test
  public void dumpQueryOnException() throws Exception {
    try (Connection con = createCon("dumpQueriesOnException")) {
      Statement stmt = con.createStatement();
      assertThrowsContains(
          SQLSyntaxErrorException.class,
          () -> stmt.execute("SELECT 'long value' FROM wrongTable"),
          "Query is: SELECT 'long value' FROM wrongTable");
    }

    try (Connection con = createCon("maxQuerySizeToLog=13&dumpQueriesOnException")) {
      Statement stmt = con.createStatement();
      assertThrowsContains(
          SQLSyntaxErrorException.class,
          () -> stmt.execute("SELECT 'long value' FROM wrongTable"),
          "Query is: SELECT 'lo...");
    }
  }

  @Test
  public void testPre41ErrorFormat() throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    SQLException exception = null;
    int max_connections;
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT @@max_connections");
    rs.next();
    max_connections = rs.getInt(1);
    Assumptions.assumeTrue(max_connections < 1000);
    Connection[] cons = new Connection[max_connections];
    for (int i = 0; i < max_connections; i++) {
      try {
        cons[i] = createCon();
      } catch (SQLException sqle) {
        exception = sqle;
      }
    }

    for (int i = 0; i < max_connections; i++) {
      try {
        if (cons[i] != null) cons[i].close();
      } catch (SQLException sqle) {
      }
    }
    assertNotNull(exception);
    assertTrue(exception.getMessage().contains("Too many"));
  }

  @Test
  public void deadLockInformation() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("insert into deadlock(a) values(0), (1)");

    try (Connection conn1 =
        createCon(
            "includeInnodbStatusInDeadlockExceptions&includeThreadDumpInDeadlockExceptions")) {

      conn1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      Statement stmt1 = conn1.createStatement();
      try {
        stmt1.execute("SET SESSION idle_transaction_timeout=2");
      } catch (SQLException e) {
        // eat ( for mariadb >= 10.3)
      }
      stmt.execute("start transaction");
      stmt.execute("update deadlock set a = 2 where a <> 0");

      try (Connection conn2 =
          createCon(
              "&includeInnodbStatusInDeadlockExceptions&includeThreadDumpInDeadlockExceptions")) {

        Statement stmt2 = conn2.createStatement();
        conn2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        try {
          stmt2.execute("SET SESSION idle_transaction_timeout=2, innodb_lock_wait_timeout=2");
        } catch (SQLException e) {
          // eat ( for mariadb >= 10.3)
        }
        stmt2.execute("start transaction");
        try {
          stmt2.execute("update deadlock set a = 3 where a <> 1");
          fail("Must have thrown deadlock exception");
        } catch (SQLException sqle) {
          assertTrue(sqle.getMessage().contains("current threads:"));
          assertTrue(sqle.getMessage().contains("deadlock information"));
        }
      }
    }
  }
}
