// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.Statement;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.BitSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ProcedureParameterTest extends Common {

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE PROCEDURE useParameterName(a int) AS BEGIN  ECHO SELECT a;  END;");
    stmt.execute(
        "CREATE PROCEDURE withStrangeParameter(a DECIMAL(10,2)) AS BEGIN  ECHO SELECT a as b;  END;");
    stmt.execute("FLUSH TABLES");
  }

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS useParameterName");
    stmt.execute("DROP PROCEDURE IF EXISTS withStrangeParameter");
  }

  @Test
  public void callUseParameterName() throws Exception {
    CallableStatement stmt = sharedConn.prepareCall("{call useParameterName(?)}");
    stmt.setInt("a", 1);
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    int res = rs.getInt(1);
    assertEquals(res, 1);
  }

  @Test
  public void callWithoutBracket() throws Exception {

    CallableStatement stmt = sharedConn.prepareCall("call useParameterName(?)");
    stmt.setInt(1, 1);
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    int res = rs.getInt(1);
    assertEquals(res, 1);
  }

  @Test
  public void callWithStrangeParameter() throws SQLException {

    try (CallableStatement call = sharedConn.prepareCall("{call withStrangeParameter(?)}")) {
      double expected = 5.43;
      call.setDouble("a", expected);
      try (ResultSet rs = call.executeQuery()) {
        assertTrue(rs.next());
        double res = rs.getDouble("b");
        assertEquals(expected, res, 0);
        // now fail due to three decimals
        double tooMuch = 34.987;
        call.setDouble("a", tooMuch);
        try (ResultSet rs2 = call.executeQuery()) {
          assertTrue(rs2.next());
          assertNotEquals(rs2.getDouble("b"), tooMuch);
        }
      }
    }
  }

  @Test
  public void basicProcedure() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS basic_proc");
    stmt.execute(
        "CREATE PROCEDURE basic_proc (t1 INT, t2 MEDIUMINT unsigned, t3 DECIMAL(8,3), t4 VARCHAR(20), t5 SMALLINT) AS BEGIN \n"
            + "END");
    try (CallableStatement callableStatement =
        sharedConn.prepareCall("{call basic_proc(?,?,?,?)}")) {
      ParameterMetaData meta = callableStatement.getParameterMetaData();
      assertEquals(5, meta.getParameterCount());
      assertEquals("int", meta.getParameterClassName(1));
      assertEquals("int", meta.getParameterClassName(2));
      assertEquals("java.math.BigDecimal", meta.getParameterClassName(3));
      assertEquals("java.lang.String", meta.getParameterClassName(4));
      assertEquals("short", meta.getParameterClassName(5));
      Common.assertThrowsContains(
          SQLException.class, () -> meta.getParameterClassName(6), "invalid parameter index 6");

      assertEquals("INT", meta.getParameterTypeName(1));
      assertEquals("MEDIUMINT", meta.getParameterTypeName(2));
      assertEquals("DECIMAL", meta.getParameterTypeName(3));
      assertEquals("VARCHAR", meta.getParameterTypeName(4));
      assertEquals("SMALLINT", meta.getParameterTypeName(5));
      Common.assertThrowsContains(
          SQLException.class, () -> meta.getParameterTypeName(0), "invalid parameter index 0");

      assertEquals(Types.INTEGER, meta.getParameterType(1));
      assertEquals(Types.INTEGER, meta.getParameterType(2));
      assertEquals(Types.DECIMAL, meta.getParameterType(3));
      assertEquals(Types.VARCHAR, meta.getParameterType(4));
      assertEquals(Types.SMALLINT, meta.getParameterType(5));
      Common.assertThrowsContains(
          SQLException.class, () -> meta.getParameterType(0), "invalid parameter index 0");

      assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(1));
      assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(2));
      assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(3));
      assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(4));
      assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(5));
      Common.assertThrowsContains(
          SQLException.class, () -> meta.getParameterMode(10), "invalid parameter index 10");

      assertEquals(10, meta.getPrecision(1));
      assertEquals(8, meta.getPrecision(3));
      if (minVersion(7, 5, 0)) {
        assertEquals(20, meta.getPrecision(4));
      }
      assertEquals(5, meta.getPrecision(5));
      Common.assertThrowsContains(
          SQLException.class, () -> meta.getPrecision(10), "invalid parameter index 10");

      assertEquals(0, meta.getScale(1));
      assertEquals(0, meta.getScale(2));
      assertEquals(3, meta.getScale(3));
      assertEquals(0, meta.getScale(4));
      assertEquals(0, meta.getScale(5));
      Common.assertThrowsContains(
          SQLException.class, () -> meta.getScale(10), "invalid parameter index 10");

      assertTrue(meta.isSigned(1));
      assertFalse(meta.isSigned(2));

      assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
      assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(2));

      assertNotNull(meta.unwrap(com.singlestore.jdbc.CallableParameterMetaData.class));
      assertNotNull(meta.unwrap(ParameterMetaData.class));
      Common.assertThrowsContains(
          SQLException.class,
          () -> meta.unwrap(String.class),
          "The receiver is not a wrapper for java.lang.String");
      assertTrue(meta.isWrapperFor(com.singlestore.jdbc.CallableParameterMetaData.class));
      assertTrue(meta.isWrapperFor(ParameterMetaData.class));
      assertFalse(meta.isWrapperFor(String.class));
    }
  }

  @Test
  public void getParameterTypeProcedure() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS procType");
    stmt.execute(
        "CREATE PROCEDURE procType ("
            + "t1 TINYINT, "
            + "t2 SMALLINT, "
            + "t3 MEDIUMINT, "
            + "t4 INT, "
            + "t5 BIGINT, "
            + "t6 DECIMAL(6,3), "
            + "t7 FLOAT, "
            + "t8 DOUBLE, "
            + "t9 BIT, "
            + "t10 CHAR(5), "
            + "t11 VARCHAR(6), "
            + "t12 BINARY(6), "
            + "t13 VARBINARY(6), "
            + "t14 TINYBLOB, "
            + "t15 BLOB, "
            + "t16 MEDIUMBLOB, "
            + "t17 LONGBLOB, "
            + "t18 TINYTEXT, "
            + "t19 TEXT, "
            + "t20 MEDIUMTEXT, "
            + "t21 LONGTEXT, "
            + "t22 ENUM('value1','value2'), "
            + "t23 DATE, "
            + "t24 TIME, "
            + "t25 TIMESTAMP, "
            + "t26 DATETIME, "
            + "t27 YEAR, "
            + "t28 NUMERIC, "
            + "t29 JSON"
            + ")\n AS BEGIN END");
    try (CallableStatement callableStatement =
        sharedConn.prepareCall(
            "{call procType(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}")) {
      ParameterMetaData meta = callableStatement.getParameterMetaData();

      String[] expectedTypeName =
          new String[] {
            "TINYINT",
            "SMALLINT",
            "MEDIUMINT",
            "INT",
            "BIGINT",
            "DECIMAL",
            "FLOAT",
            "DOUBLE",
            "BIT",
            "CHAR",
            "VARCHAR",
            "BINARY",
            "VARBINARY",
            "TINYBLOB",
            "BLOB",
            "MEDIUMBLOB",
            "LONGBLOB",
            "TINYTEXT",
            "TEXT",
            "MEDIUMTEXT",
            "LONGTEXT",
            "ENUM",
            "DATE",
            "TIME",
            "TIMESTAMP",
            "DATETIME",
            "YEAR",
            "DECIMAL",
            "JSON"
          };
      int[] expectedType =
          new int[] {
            Types.TINYINT,
            Types.SMALLINT,
            Types.INTEGER,
            Types.INTEGER,
            Types.BIGINT,
            Types.DECIMAL,
            Types.FLOAT,
            Types.DOUBLE,
            Types.BIT,
            Types.CHAR,
            Types.VARCHAR,
            Types.BINARY,
            Types.VARBINARY,
            Types.BLOB,
            Types.BLOB,
            Types.BLOB,
            Types.BLOB,
            Types.CLOB,
            Types.CLOB,
            Types.CLOB,
            Types.CLOB,
            Types.VARCHAR,
            Types.DATE,
            Types.TIME,
            Types.TIMESTAMP,
            Types.TIMESTAMP,
            Types.SMALLINT,
            Types.DECIMAL,
            Types.CLOB
          };

      String[] expectedClass =
          new String[] {
            byte.class.getName(),
            short.class.getName(),
            int.class.getName(),
            int.class.getName(),
            long.class.getName(),
            BigDecimal.class.getName(),
            float.class.getName(),
            double.class.getName(),
            BitSet.class.getName(),
            String.class.getName(),
            String.class.getName(),
            byte[].class.getName(),
            byte[].class.getName(),
            byte[].class.getName(),
            Blob.class.getName(),
            Blob.class.getName(),
            Blob.class.getName(),
            String.class.getName(),
            Clob.class.getName(),
            Clob.class.getName(),
            Clob.class.getName(),
            String.class.getName(),
            Date.class.getName(),
            Time.class.getName(),
            Timestamp.class.getName(),
            Timestamp.class.getName(),
            short.class.getName(),
            BigDecimal.class.getName(),
            Clob.class.getName()
          };
      for (int i = 1; i < 30; i++) {
        assertEquals(expectedTypeName[i - 1], meta.getParameterTypeName(i));
        assertEquals(expectedType[i - 1], meta.getParameterType(i));
        assertEquals(expectedClass[i - 1], meta.getParameterClassName(i));
      }
    }
  }
}
