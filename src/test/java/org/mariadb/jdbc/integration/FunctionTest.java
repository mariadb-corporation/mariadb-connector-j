// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.CallableParameterMetaData;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;

public class FunctionTest extends Common {

  @Test
  public void basicFunction() throws SQLException {
    // disabled for mysql : see https://bugs.mysql.com/bug.php?id=108545
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP FUNCTION IF EXISTS basic_function");
    stmt.execute(
        "CREATE FUNCTION basic_function (t1 INT, t2 INT unsigned) RETURNS INT DETERMINISTIC RETURN"
            + " t1 * t2;");
    try (CallableStatement callableStatement =
        sharedConn.prepareCall("{? = call basic_function(?,?)}")) {
      callableStatement.registerOutParameter(1, JDBCType.INTEGER);
      callableStatement.setInt(2, 2);
      callableStatement.setInt(3, 3);
      callableStatement.execute();

      assertEquals(6, callableStatement.getInt(1));

      callableStatement.clearParameters();
      callableStatement.setInt(2, 3);
      callableStatement.setInt(3, 3);
      callableStatement.execute();
      assertEquals(9, callableStatement.getInt(1));

      callableStatement.clearParameters();
      assertThrowsContains(
          SQLTransientConnectionException.class,
          callableStatement::execute,
          "Parameter at position 1 is not set");
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
    stmt.execute("CREATE FUNCTION no_arg_function () RETURNS DOUBLE DETERMINISTIC RETURN RAND();");
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
    stmt.execute("CREATE FUNCTION parameter_meta () RETURNS DOUBLE DETERMINISTIC RETURN RAND();");
    try (CallableStatement callableStatement =
        sharedConn.prepareCall("{? = call parameter_meta()}")) {
      Common.assertThrowsContains(
          SQLSyntaxErrorException.class,
          () -> callableStatement.registerOutParameter(-1, JDBCType.DOUBLE),
          "wrong parameter index");
      callableStatement.registerOutParameter(1, JDBCType.DOUBLE);

      // XPAND doesn't support I_S.parameters: https://jira.mariadb.org/browse/XPT-267
      if (!isXpand()) {
        CallableParameterMetaData meta =
            (CallableParameterMetaData) callableStatement.getParameterMetaData();
        assertEquals(org.mariadb.jdbc.ParameterMetaData.parameterModeOut, meta.getParameterMode(1));
        assertNull(meta.getParameterName(1));
      }
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
    stmt.execute("CREATE FUNCTION no_arg_function () RETURNS DOUBLE DETERMINISTIC RETURN RAND();");
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
      if (!isXpand()) {
        Common.assertThrowsContains(
            SQLSyntaxErrorException.class,
            () -> callableStatement.registerOutParameter("r", JDBCType.DOUBLE),
            "parameter name r not found");
      }
      Common.assertThrowsContains(
          SQLSyntaxErrorException.class,
          () -> callableStatement.registerOutParameter(2, JDBCType.DOUBLE),
          "wrong parameter index");
    }
  }

  @Test
  public void functionToString() throws SQLException {
    try (CallableStatement callableStatement =
        sharedConn.prepareCall("{? = call basic_function(?,?)}")) {

      assertEquals(
          "FunctionStatement{sql:'SELECT basic_function(?,?)', parameters:[<OUT>null]}",
          callableStatement.toString());
      callableStatement.setLong(2, 10L);
      callableStatement.setBytes(3, new byte[] {(byte) 'a', (byte) 'b'});
      assertEquals(
          "FunctionStatement{sql:'SELECT basic_function(?,?)', parameters:[<OUT>null,10,_binary"
              + " 'ab']}",
          callableStatement.toString());
    }
  }
}
