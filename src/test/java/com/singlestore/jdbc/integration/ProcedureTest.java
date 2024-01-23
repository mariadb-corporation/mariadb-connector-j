// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.CallableParameterMetaData;
import com.singlestore.jdbc.Statement;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ProcedureTest extends Common {

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS procedure_test");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE procedure_test (t0 int)");
  }

  @Test
  public void wrongCall() throws SQLException {
    Common.assertThrowsContains(
        SQLException.class, () -> sharedConn.prepareCall("SELECT ?"), "invalid callable syntax");
  }

  @Test
  public void prepInsert() throws SQLException {
    Statement st = sharedConn.createStatement();
    st.execute("DROP PROCEDURE IF EXISTS prep_proc2");
    st.execute(
        "CREATE PROCEDURE prep_proc2 (t1 INT) AS BEGIN \n"
            + "INSERT INTO procedure_test(t0) VALUE (t1);\n"
            + "END");

    try (PreparedStatement stmt = sharedConn.prepareCall("CALL prep_proc2(?)")) {
      stmt.setInt(1, 1);
      stmt.execute();
    }
  }

  @Test
  public void prep() throws SQLException {
    Statement st = sharedConn.createStatement();
    st.execute("DROP PROCEDURE IF EXISTS prep_proc");
    st.execute("CREATE PROCEDURE prep_proc (t1 INT) AS BEGIN \n" + "ECHO SELECT t1;\n" + "END");

    try (PreparedStatement stmt = sharedConn.prepareCall("CALL prep_proc(?)")) {
      assertEquals(ResultSet.TYPE_FORWARD_ONLY, stmt.getResultSetType());
      assertEquals(ResultSet.CONCUR_READ_ONLY, stmt.getResultSetConcurrency());
      assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
      assertEquals(sharedConn, stmt.getConnection());
    }

    try (PreparedStatement stmt =
        sharedConn.prepareCall(
            "CALL prep_proc(?)", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
      assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, stmt.getResultSetType());
      assertEquals(ResultSet.CONCUR_UPDATABLE, stmt.getResultSetConcurrency());
      assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
      assertEquals(sharedConn, stmt.getConnection());
    }

    try (PreparedStatement stmt =
        sharedConn.prepareCall(
            "CALL prep_proc(?)",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE,
            ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
      assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, stmt.getResultSetType());
      assertEquals(ResultSet.CONCUR_UPDATABLE, stmt.getResultSetConcurrency());
      // not supported
      assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
      assertEquals(sharedConn, stmt.getConnection());
    }
  }

  @Test
  @SuppressWarnings("deprecated")
  public void basicProcedure1() throws Throwable {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS reading");
    stmt.execute("CREATE TABLE reading(dt DATETIME(6), sensorID INT, value FLOAT)");
    stmt.execute("INSERT reading VALUES(NOW(6), 1, 5.0);");
    stmt.execute("INSERT reading VALUES(NOW(6), 1, 5.5);");
    stmt.execute("INSERT reading VALUES(NOW(6), 2, 4.0);");

    stmt.execute(
        "CREATE OR REPLACE PROCEDURE getLastReading(_sensorID INT) RETURNS FLOAT AS DECLARE \n"
            + "q query(value FLOAT) = SELECT value FROM reading WHERE sensorID = _sensorID AND dt = (SELECT max(dt) FROM reading WHERE sensorID = _sensorID); \n"
            + "BEGIN RETURN SCALAR(q); END");
    stmt.execute(
        "CREATE OR REPLACE PROCEDURE getLastReadings() AS DECLARE \n"
            + "r1 FLOAT; r2 FLOAT; \n"
            + "BEGIN r1 = getLastReading(1); r2 = getLastReading(2); ECHO SELECT(CONCAT('reading 1 = ', r1 , '; reading 2 = ', r2)) AS msg; END");
    try (CallableStatement callableStatement = sharedConn.prepareCall("{call getLastReadings()}")) {
      ResultSet rs = callableStatement.executeQuery();
      assertTrue(rs.next());
      assertEquals("reading 1 = 5.5; reading 2 = 4", rs.getString(1));
    }
  }

  @Test
  public void basicProcedure2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE OR REPLACE PROCEDURE basicProcedure2 (t1 INT) AS BEGIN \n"
            + "ECHO SELECT t1;\n"
            + "END");
    try (CallableStatement callableStatement =
        sharedConn.prepareCall("{call basicProcedure2(?)}")) {
      callableStatement.setInt(1, 2);
      ResultSet rs = callableStatement.executeQuery();
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
    }

    try (CallableStatement callableStatement =
        sharedConn.prepareCall("{call basicProcedure2(?)}")) {
      callableStatement.setInt("t1", 3);
      ResultSet rs = callableStatement.executeQuery();
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
    }
  }

  @Test
  public void parameterMeta() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE OR REPLACE PROCEDURE parameter_meta (t1 VARCHAR(30), t2 INT, t3 TINYINT, t4 DOUBLE) RETURNS DOUBLE AS BEGIN RETURN RAND(); END");
    try (CallableStatement callableStatement =
        sharedConn.prepareCall("{? = call parameter_meta(?, ?, ?, ?)}")) {
      CallableParameterMetaData meta =
          (CallableParameterMetaData) callableStatement.getParameterMetaData();

      assertEquals(
          com.singlestore.jdbc.ParameterMetaData.parameterModeIn, meta.getParameterMode(1));
      assertEquals("t1", meta.getParameterName(1));
      assertEquals(Types.VARCHAR, meta.getParameterType(1));

      assertEquals(
          com.singlestore.jdbc.ParameterMetaData.parameterModeIn, meta.getParameterMode(2));
      assertEquals("t2", meta.getParameterName(2));
      assertEquals(Types.INTEGER, meta.getParameterType(2));

      assertEquals(
          com.singlestore.jdbc.ParameterMetaData.parameterModeIn, meta.getParameterMode(3));
      assertEquals("t3", meta.getParameterName(3));
      assertEquals(Types.TINYINT, meta.getParameterType(3));

      assertEquals(
          com.singlestore.jdbc.ParameterMetaData.parameterModeIn, meta.getParameterMode(4));
      assertEquals("t4", meta.getParameterName(4));
      assertEquals(Types.DOUBLE, meta.getParameterType(4));
    }
  }
}
