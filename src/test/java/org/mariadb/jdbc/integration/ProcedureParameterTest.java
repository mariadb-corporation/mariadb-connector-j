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

import java.math.BigDecimal;
import java.sql.*;
import java.util.BitSet;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Statement;

public class ProcedureParameterTest extends Common {

  @Test
  public void basicProcedure() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS basic_proc");
    stmt.execute(
        "CREATE PROCEDURE basic_proc (INOUT t1 INT, IN t2 MEDIUMINT unsigned, OUT t3 DECIMAL(8,3), OUT t4 VARCHAR(20), IN t5 SMALLINT) BEGIN \n"
            + "set t3 = t1 * t5;\n"
            + "set t1 = t2 * t1;\n"
            + "set t4 = 'return data';\n"
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
      assertThrowsContains(
          SQLException.class, () -> meta.getParameterClassName(6), "invalid parameter index 6");

      assertEquals("INT", meta.getParameterTypeName(1));
      assertEquals("MEDIUMINT", meta.getParameterTypeName(2));
      assertEquals("DECIMAL", meta.getParameterTypeName(3));
      assertEquals("VARCHAR", meta.getParameterTypeName(4));
      assertEquals("SMALLINT", meta.getParameterTypeName(5));
      assertThrowsContains(
          SQLException.class, () -> meta.getParameterTypeName(0), "invalid parameter index 0");

      assertEquals(Types.INTEGER, meta.getParameterType(1));
      assertEquals(Types.INTEGER, meta.getParameterType(2));
      assertEquals(Types.DECIMAL, meta.getParameterType(3));
      assertEquals(Types.VARCHAR, meta.getParameterType(4));
      assertEquals(Types.SMALLINT, meta.getParameterType(5));
      assertThrowsContains(
          SQLException.class, () -> meta.getParameterType(0), "invalid parameter index 0");

      assertEquals(ParameterMetaData.parameterModeInOut, meta.getParameterMode(1));
      assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(2));
      assertEquals(ParameterMetaData.parameterModeOut, meta.getParameterMode(3));
      assertEquals(ParameterMetaData.parameterModeOut, meta.getParameterMode(4));
      assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(5));
      assertThrowsContains(
          SQLException.class, () -> meta.getParameterMode(10), "invalid parameter index 10");

      assertEquals(10, meta.getPrecision(1));
      assertEquals(8, meta.getPrecision(3));
      assertEquals(20, meta.getPrecision(4));
      assertEquals(5, meta.getPrecision(5));
      assertThrowsContains(
          SQLException.class, () -> meta.getPrecision(10), "invalid parameter index 10");

      assertEquals(0, meta.getScale(1));
      assertEquals(0, meta.getScale(2));
      assertEquals(3, meta.getScale(3));
      assertEquals(0, meta.getScale(4));
      assertEquals(0, meta.getScale(5));
      assertThrowsContains(
          SQLException.class, () -> meta.getScale(10), "invalid parameter index 10");

      assertTrue(meta.isSigned(1));
      assertFalse(meta.isSigned(2));

      assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
      assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(2));

      assertNotNull(meta.unwrap(org.mariadb.jdbc.CallableParameterMetaData.class));
      assertNotNull(meta.unwrap(ParameterMetaData.class));
      assertThrowsContains(
          SQLException.class,
          () -> meta.unwrap(String.class),
          "The receiver is not a wrapper for java.lang.String");
      assertTrue(meta.isWrapperFor(org.mariadb.jdbc.CallableParameterMetaData.class));
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
            + "IN t1 TINYINT, "
            + "IN t2 SMALLINT, "
            + "IN t3 MEDIUMINT, "
            + "IN t4 INT, "
            + "IN t5 BIGINT, "
            + "IN t6 DECIMAL(6,3), "
            + "IN t7 FLOAT, "
            + "IN t8 DOUBLE, "
            + "IN t9 BIT, "
            + "IN t10 CHAR(5), "
            + "IN t11 VARCHAR(6), "
            + "IN t12 BINARY(6), "
            + "IN t13 VARBINARY(6), "
            + "IN t14 TINYBLOB, "
            + "IN t15 BLOB, "
            + "IN t16 MEDIUMBLOB, "
            + "IN t17 LONGBLOB, "
            + "IN t18 TINYTEXT, "
            + "IN t19 TEXT, "
            + "IN t20 MEDIUMTEXT, "
            + "IN t21 LONGTEXT, "
            + "IN t22 ENUM('value1','value2'), "
            + "IN t23 DATE, "
            + "IN t24 TIME, "
            + "IN t25 TIMESTAMP, "
            + "IN t26 DATETIME, "
            + "IN t27 YEAR, "
            + "IN t28 NUMERIC "
            + ")\n BEGIN \n"
            + "SELECT 1;\n"
            + "END");
    try (CallableStatement callableStatement =
        sharedConn.prepareCall(
            "{call procType(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}")) {
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
            "DECIMAL"
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
            Types.VARCHAR,
            Types.CLOB,
            Types.CLOB,
            Types.CLOB,
            Types.VARCHAR,
            Types.DATE,
            Types.TIME,
            Types.TIMESTAMP,
            Types.TIMESTAMP,
            Types.SMALLINT,
            Types.DECIMAL
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
            BigDecimal.class.getName()
          };
      for (int i = 1; i < 29; i++) {
        assertEquals(expectedTypeName[i - 1], meta.getParameterTypeName(i));
        assertEquals(expectedType[i - 1], meta.getParameterType(i));
        assertEquals(expectedClass[i - 1], meta.getParameterClassName(i));
      }
    }
  }
}
