// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Common;
import com.singlestore.jdbc.Statement;
import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ParameterMetaDataTest extends Common {

  @AfterAll
  public static void after2() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE IF EXISTS parameter_meta");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    after2();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE parameter_meta ("
            + "id int not null primary key auto_increment, "
            + "test varchar(20), "
            + "t2 DECIMAL(10,3))");
  }

  @Test
  public void parameterMetaDataTypeNotAvailable() throws Exception {
    try (com.singlestore.jdbc.Connection con = createCon("&useServerPrepStmts=false")) {
      parameterMetaDataTypeNotAvailable(con, true);
    }
    try (com.singlestore.jdbc.Connection con = createCon("&useServerPrepStmts")) {
      parameterMetaDataTypeNotAvailable(con, false);
    }
  }

  private void parameterMetaDataTypeNotAvailable(com.singlestore.jdbc.Connection con, boolean text)
      throws SQLException {
    String query = "SELECT * FROM parameter_meta WHERE test = ? and id = ?";
    try (PreparedStatement prepStmt = con.prepareStatement(query)) {
      ParameterMetaData parameterMetaData = prepStmt.getParameterMetaData();
      assertEquals(2, parameterMetaData.getParameterCount());
      assertThrowsContains(
          SQLException.class,
          () -> parameterMetaData.getParameterType(1),
          "Getting parameter type metadata are not supported");
      assertThrowsContains(
          SQLException.class, () -> parameterMetaData.isNullable(-1), "Wrong index position");
      assertThrowsContains(
          SQLException.class, () -> parameterMetaData.isNullable(3), "Wrong index position");
    }
  }

  @Test
  public void parameterMetaDataNotPreparable() throws Exception {
    try (com.singlestore.jdbc.Connection con = createCon("&useServerPrepStmts=false")) {
      parameterMetaDataNotPreparable(con, true);
    }
    try (com.singlestore.jdbc.Connection con = createCon("&useServerPrepStmts")) {
      parameterMetaDataNotPreparable(con, false);
    }
  }

  private void parameterMetaDataNotPreparable(com.singlestore.jdbc.Connection con, boolean text)
      throws SQLException {
    // statement that cannot be prepared
    try (PreparedStatement pstmt =
        con.prepareStatement("select  TMP.field1 from (select ? from dual) TMP")) {
      try {
        pstmt.getParameterMetaData();
        fail();
      } catch (SQLSyntaxErrorException e) {
        // eat
      }
    } catch (SQLSyntaxErrorException e) {
      // eat
    }
  }

  @Test
  public void parameterMetaDataBasic() throws SQLException {
    String query = "SELECT * FROM parameter_meta WHERE test = ? and id = ? and t2 = ?";
    // Parameter type are not sent by server.
    // See https://jira.mariadb.org/browse/CONJ-568 and https://jira.mariadb.org/browse/MDEV-15031
    // so only basic info like parameter number are retrieved.
    try (PreparedStatement prepStmt = sharedConnBinary.prepareStatement(query)) {
      prepStmt.setString(1, "");
      prepStmt.setInt(2, 1);
      prepStmt.setInt(3, 1);
      prepStmt.executeQuery();
      ParameterMetaData meta = prepStmt.getParameterMetaData();
      assertEquals(3, meta.getParameterCount());
      assertEquals(0, meta.getPrecision(1));
      assertEquals(0, meta.getScale(1));

      assertTrue(meta.isSigned(1));
      assertEquals(ParameterMetaData.parameterNullable, meta.isNullable(1));
      assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(1));

      assertThrowsContains(
          SQLFeatureNotSupportedException.class,
          () -> meta.getParameterType(1),
          "Getting parameter type " + "metadata are not supported");
      assertThrowsContains(
          SQLFeatureNotSupportedException.class,
          () -> meta.getParameterClassName(1),
          "Unknown parameter metadata class name");

      assertThrowsContains(
          SQLException.class,
          () -> meta.getScale(20),
          "Wrong index position. Is 20 but must be in 1-3 range");
      assertThrowsContains(
          SQLException.class,
          () -> meta.getPrecision(20),
          "Wrong index position. Is 20 but must be in 1-3 range");
      assertThrowsContains(
          SQLException.class,
          () -> meta.isSigned(20),
          "Wrong index position. Is 20 but must be in 1-3 range");
      assertThrowsContains(
          SQLException.class,
          () -> meta.isNullable(20),
          "Wrong index position. Is 20 but must be in 1-3 range");
      assertThrowsContains(
          SQLException.class,
          () -> meta.getParameterTypeName(20),
          "Wrong index position. Is 20 but must be in 1-3 range");
      assertThrowsContains(
          SQLException.class,
          () -> meta.getParameterMode(20),
          "Wrong index position. Is 20 but must be in 1-3 range");

      meta.unwrap(java.sql.ParameterMetaData.class);
      assertThrowsContains(
          SQLException.class,
          () -> meta.unwrap(String.class),
          "The receiver is not a wrapper for java.lang.String");
    }
  }
}
