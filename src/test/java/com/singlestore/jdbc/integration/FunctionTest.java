// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.CallableParameterMetaData;
import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.Statement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.jupiter.api.Test;

public class FunctionTest extends Common {

  @Test
  public void basicFunction() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP FUNCTION IF EXISTS basic_function");
    stmt.execute(
        "CREATE FUNCTION basic_function (t1 INT, t2 INT unsigned) RETURNS INT AS BEGIN RETURN t1 * t2; END");
    try (CallableStatement callableStatement =
        sharedConn.prepareCall("{? = call basic_function(?,?)}")) {
      callableStatement.setInt(1, 2);
      callableStatement.setInt(2, 3);
      ResultSet rs = callableStatement.executeQuery();
      assertTrue(rs.next());
      assertEquals(6, rs.getInt(1));
    }

    try (CallableStatement callableStatement =
        sharedConn.prepareCall("{? = call basic_function(?,?)}")) {
      callableStatement.setInt("t1", 3);
      callableStatement.setInt("t2", 3);
      ResultSet rs = callableStatement.executeQuery();
      assertTrue(rs.next());
      assertEquals(9, rs.getInt(1));
    }
  }

  @Test
  public void functionWithoutArg() throws SQLException {
    functionWithoutArg(sharedConn);
    functionWithoutArg(sharedConnBinary);
  }

  private void functionWithoutArg(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("DROP FUNCTION IF EXISTS no_arg_function");
    stmt.execute("CREATE FUNCTION no_arg_function () RETURNS DOUBLE AS BEGIN RETURN RAND(); END");
    try (CallableStatement callableStatement = con.prepareCall("{? = call no_arg_function()}")) {
      ResultSet rs = callableStatement.executeQuery();
      assertTrue(rs.next());
      rs.getDouble(1);
    }
  }

  @Test
  public void parameterMeta() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP FUNCTION IF EXISTS parameter_meta_func");
    stmt.execute(
        "CREATE FUNCTION parameter_meta_func (t1 VARCHAR(30), t2 INT, t3 TINYINT, t4 DOUBLE) RETURNS DOUBLE AS BEGIN RETURN RAND(); END");
    try (CallableStatement callableStatement =
        sharedConn.prepareCall("{? = call parameter_meta_func(?, ?, ?, ?)}")) {
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
