// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;

import org.junit.jupiter.api.*;

public class ErrorTest extends Common {

  @BeforeAll
  public static void begin() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE deadlock(a int primary key)");
    stmt.execute("CREATE TABLE deadlock2(a int primary key) ENGINE=InnoDB");

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
      try {
        stmt.execute("SELECT 'long value' FROM wrongTable");
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Query is: SELECT 'long value' FROM wrongTable"));
      }
    }

    try (Connection con = createCon("maxQuerySizeToLog=100&dumpQueriesOnException")) {
      Statement stmt = con.createStatement();
      try {
        stmt.execute("SELECT 'long value' FROM wrongTable");
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Query is: SELECT 'long value' FROM wrongTable"));
      }
    }

    try (Connection con = createCon("maxQuerySizeToLog=13&dumpQueriesOnException")) {
      Statement stmt = con.createStatement();
      try {
        stmt.execute("SELECT 'long value' FROM wrongTable");
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Query is: SELECT 'lo..."));
      }
    }
  }

  @Test
  public void testPre41ErrorFormat() throws Exception {
    testPre41ErrorFormat(sharedConn);
    try (Connection con =
        createCon("dumpQueriesOnException&includeInnodbStatusInDeadlockExceptions")) {
      testPre41ErrorFormat(con);
    }
  }

  private void testPre41ErrorFormat(Connection con) throws Exception {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv"))
            && !isXpand());
    SQLException exception = null;
    int max_connections;
    Statement stmt = con.createStatement();
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
        // eat
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
      Assumptions.assumeFalse(isXpand());
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
  @Test
  public void connectionErrorFormat() throws SQLException {
    try {
      DriverManager.getConnection("jdbc:mariadb://localhost:3000/db");
      fail("Must have thrown an error");
    } catch (SQLException e) {
      assertEquals("08000", e.getSQLState());
    }
  }
}
