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
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;

public class FunctionTest extends Common {

  @Test
  public void basicFunction() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP FUNCTION IF EXISTS basic_function");
    stmt.execute(
        "CREATE FUNCTION basic_function (t1 INT, t2 INT unsigned) RETURNS INT DETERMINISTIC RETURN t1 * t2;");
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
    stmt.execute("CREATE FUNCTION no_arg_function () RETURNS DOUBLE DETERMINISTIC RETURN RAND();");
    try (CallableStatement callableStatement = con.prepareCall("{? = call no_arg_function()}")) {
      callableStatement.registerOutParameter(1, JDBCType.DOUBLE);
      callableStatement.execute();
      callableStatement.getDouble(1);
      assertThrowsContains(
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
}
