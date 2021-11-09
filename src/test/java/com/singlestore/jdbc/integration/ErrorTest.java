// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Common;
import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ErrorTest extends Common {

  @BeforeAll
  public static void begin() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE deadlock(a int)");
    stmt.execute("CREATE TABLE deadlock2(a int)");

    stmt.execute("FLUSH TABLES");
  }

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS deadlock");
    stmt.execute("DROP TABLE IF EXISTS deadlock2");
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

    try (Connection con = createCon("maxQuerySizeToLog=100&dumpQueriesOnException")) {
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
  public void deadLockInformation() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("insert into deadlock(a) values(0), (1)");
    stmt.execute("SET GLOBAL lock_wait_timeout=1");

    try (Connection conn1 = createCon("includeThreadDumpInDeadlockExceptions")) {

      conn1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      Statement stmt1 = conn1.createStatement();
      stmt1.execute("start transaction");
      stmt1.execute("update deadlock set a = 3 where a = 0");

      try (Connection conn2 = createCon("&includeThreadDumpInDeadlockExceptions")) {

        Statement stmt2 = conn2.createStatement();
        conn2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        stmt2.execute("start transaction");
        try {
          stmt2.execute("update deadlock set a = 4 where a = 0");
          fail("Must have thrown deadlock exception");
        } catch (SQLException sqle) {
          assertTrue(sqle.getMessage().contains("current threads:"));
        }
      }
    } finally {
      stmt.execute("SET GLOBAL lock_wait_timeout=60");
    }
  }
}
