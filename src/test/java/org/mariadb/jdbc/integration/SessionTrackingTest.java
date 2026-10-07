// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;

/**
 * The driver relies on session tracking for its UTF-8 invariants (character_set_client,
 * character_set_results) and for the current database, whatever the server's global tracking
 * configuration.
 */
public class SessionTrackingTest extends Common {

  @BeforeAll
  public static void beforeAll2() {
    Assumptions.assumeTrue(
        (isMariaDBServer() && minVersion(10, 2, 2)) || (!isMariaDBServer() && minVersion(5, 7, 0)));
    Assumptions.assumeFalse("maxscale".equals(System.getenv("srv")));
  }

  /** Set a global variable, skipping the test when the user lacks the privilege. */
  private static void setGlobal(String variable, String value) throws SQLException {
    try (Statement stmt = sharedConn.createStatement()) {
      stmt.execute("SET GLOBAL " + variable + "=" + value);
    } catch (SQLException e) {
      Assumptions.assumeTrue(e.getErrorCode() != 1227, "no privilege to set global " + variable);
      throw e;
    }
  }

  private static String getGlobal(String variable) throws SQLException {
    try (ResultSet rs = sharedConn.createStatement().executeQuery("SELECT @@global." + variable)) {
      rs.next();
      return rs.getString(1);
    }
  }

  private static void assertCharsetGuards(Connection con, String label) throws SQLException {
    try (Statement stmt = con.createStatement()) {
      SQLNonTransientConnectionException e =
          assertThrows(
              SQLNonTransientConnectionException.class,
              () -> stmt.execute("SET NAMES latin1"),
              label);
      assertTrue(e.getMessage().contains("character set was changed to 'latin1'"), e.getMessage());
      assertEquals("08000", e.getSQLState());
    }
    assertTrue(con.isClosed());
  }

  @Test
  public void clientCharsetChangeClosesConnection() throws SQLException {
    try (Connection con = createCon()) {
      assertCharsetGuards(con, "default");
    }
  }

  @Test
  public void resultsCharsetChangeClosesConnection() throws SQLException {
    try (Connection con = createCon();
        Statement stmt = con.createStatement()) {
      SQLNonTransientConnectionException e =
          assertThrows(
              SQLNonTransientConnectionException.class,
              () -> stmt.execute("SET character_set_results=latin1"));
      assertTrue(
          e.getMessage().contains("character_set_results was changed to 'latin1'"), e.getMessage());
      assertTrue(con.isClosed());
    }
  }

  @Test
  public void utf8AndNoConversionResultsAllowed() throws SQLException {
    try (Connection con = createCon();
        Statement stmt = con.createStatement()) {
      stmt.execute("SET character_set_results=utf8mb4");
      stmt.execute("SET character_set_results=NULL");
      stmt.execute("SET NAMES utf8mb4");
      try (ResultSet rs = stmt.executeQuery("SELECT 'héllo'")) {
        assertTrue(rs.next());
        assertEquals("héllo", rs.getString(1));
      }
      assertFalse(con.isClosed());
    }
  }

  /** The guard must not depend on what the DBA left in the global tracked list. */
  @Test
  public void guardIndependentOfGlobalTrackedList() throws SQLException {
    String initial = getGlobal("session_track_system_variables");
    try {
      // the driver sets its own list, so the connection works whatever the global value. But
      // MariaDB only enables its variable tracker at session start when the list is not empty
      // (Session_sysvars_tracker::enable), so with an empty global the guard cannot work there
      setGlobal("session_track_system_variables", "''");
      try (Connection con = createCon()) {
        if (isMariaDBServer()) {
          assertTrue(con.isValid(1));
        } else {
          assertCharsetGuards(con, "global ''");
        }
      }

      for (String global : new String[] {"'autocommit'", "'Autocommit, time_zone'", "'*'"}) {
        setGlobal("session_track_system_variables", global);
        try (Connection con = createCon()) {
          assertCharsetGuards(con, "global " + global);
        }
        setGlobal("session_track_system_variables", global);
        try (Connection con = createCon();
            Statement stmt = con.createStatement()) {
          assertThrows(SQLException.class, () -> stmt.execute("SET character_set_results=latin1"));
          assertTrue(con.isClosed());
        }
      }
    } finally {
      setGlobal("session_track_system_variables", "'" + initial + "'");
    }
  }

  /** The current database must follow USE even when schema tracking is disabled globally. */
  @Test
  public void schemaTrackingForced() throws SQLException {
    String initial = getGlobal("session_track_schema");
    try {
      setGlobal("session_track_schema", "0");
      try (Connection con = createCon();
          Statement stmt = con.createStatement()) {
        stmt.execute("DROP DATABASE IF EXISTS _session_track_db");
        stmt.execute("CREATE DATABASE _session_track_db");
        try {
          assertEquals(database, con.getCatalog());
          stmt.execute("USE _session_track_db");
          assertEquals("_session_track_db", con.getCatalog());
        } finally {
          stmt.execute("DROP DATABASE _session_track_db");
        }
      }
    } finally {
      setGlobal("session_track_schema", "1".equals(initial) || "ON".equals(initial) ? "1" : "0");
    }
  }
}
