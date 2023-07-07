// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.CallableParameterMetaData;
import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.Statement;
import java.sql.*;
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
      callableStatement.registerOutParameter(1, JDBCType.INTEGER);
      callableStatement.setInt(2, 2);
      callableStatement.setInt(3, 3);
      callableStatement.execute();
      assertEquals(6, callableStatement.getInt(1));
    }

    try (CallableStatement callableStatement =
        sharedConn.prepareCall("{? = call basic_function(?,?)}")) {
      callableStatement.setInt(2, 2);
      callableStatement.setInt(3, 3);
      callableStatement.execute();
      assertEquals(6, callableStatement.getInt(1));
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
      callableStatement.registerOutParameter(1, JDBCType.DOUBLE);
      callableStatement.execute();
      callableStatement.getDouble(1);
      Common.assertThrowsContains(
          SQLException.class,
          () -> callableStatement.registerOutParameter(2, JDBCType.DOUBLE),
          " wrong parameter index 2");
    }

    try (CallableStatement callableStatement = con.prepareCall("{? = call no_arg_function()}")) {
      callableStatement.execute();
      callableStatement.getDouble(1);
    }

    try (CallableStatement callableStatement = con.prepareCall("{? = call no_arg_function}")) {
      callableStatement.execute();
      callableStatement.getDouble(1);
    }
  }

  @Test
  public void parameterMeta() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP FUNCTION IF EXISTS parameter_meta");
    stmt.execute("CREATE FUNCTION parameter_meta () RETURNS DOUBLE AS BEGIN RETURN RAND(); END");
    try (CallableStatement callableStatement =
        sharedConn.prepareCall("{? = call parameter_meta()}")) {
      Common.assertThrowsContains(
          SQLSyntaxErrorException.class,
          () -> callableStatement.registerOutParameter(-1, JDBCType.DOUBLE),
          "wrong parameter index");
      callableStatement.registerOutParameter(1, JDBCType.DOUBLE);

      CallableParameterMetaData meta =
          (CallableParameterMetaData) callableStatement.getParameterMetaData();
      assertEquals(
          com.singlestore.jdbc.ParameterMetaData.parameterModeOut, meta.getParameterMode(1));
      assertNull(meta.getParameterName(1));
      assertEquals(Types.DOUBLE, meta.getParameterType(1));
    } finally {
      stmt.execute("DROP FUNCTION IF EXISTS parameter_meta");
    }
  }

  @Test
  public void functionError() throws SQLException {
    functionError(sharedConn);
    functionError(sharedConnBinary);
  }

  private void functionError(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("DROP FUNCTION IF EXISTS no_arg_function");
    stmt.execute("CREATE FUNCTION no_arg_function () RETURNS DOUBLE AS BEGIN RETURN RAND(); END");
    try (CallableStatement callableStatement = con.prepareCall("{? = call no_arg_function()}")) {
      Common.assertThrowsContains(
          SQLSyntaxErrorException.class,
          () -> callableStatement.registerOutParameter(-1, JDBCType.DOUBLE),
          "wrong parameter index");
      callableStatement.registerOutParameter(1, JDBCType.DOUBLE);

      Common.assertThrowsContains(
          SQLSyntaxErrorException.class,
          () -> callableStatement.registerOutParameter(-1, JDBCType.DOUBLE),
          "wrong parameter index");
      callableStatement.registerOutParameter(1, JDBCType.DOUBLE);
      Common.assertThrowsContains(
          SQLSyntaxErrorException.class,
          () -> callableStatement.registerOutParameter(2, JDBCType.DOUBLE),
          "wrong parameter index");

      callableStatement.execute();
      Common.assertThrowsContains(
          SQLSyntaxErrorException.class,
          () -> callableStatement.registerOutParameter(-1, JDBCType.DOUBLE),
          "wrong parameter index");
      callableStatement.registerOutParameter(1, JDBCType.DOUBLE);
      Common.assertThrowsContains(
          SQLSyntaxErrorException.class,
          () -> callableStatement.registerOutParameter("r", JDBCType.DOUBLE),
          "parameter name r not found");
      Common.assertThrowsContains(
          SQLSyntaxErrorException.class,
          () -> callableStatement.registerOutParameter(2, JDBCType.DOUBLE),
          "wrong parameter index");
    }
  }
}
