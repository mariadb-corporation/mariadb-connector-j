// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;

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
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts=false")) {
      parameterMetaDataTypeNotAvailable(con);
    }
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts")) {
      parameterMetaDataTypeNotAvailable(con);
    }
  }

  private void parameterMetaDataTypeNotAvailable(org.mariadb.jdbc.Connection con)
      throws SQLException {
    String query = "SELECT * FROM parameter_meta WHERE test = ? and id = ?";
    try (PreparedStatement prepStmt = con.prepareStatement(query)) {
      ParameterMetaData parameterMetaData = prepStmt.getParameterMetaData();
      assertEquals(2, parameterMetaData.getParameterCount());
      Common.assertThrowsContains(
          SQLException.class,
          () -> parameterMetaData.getParameterType(1),
          "Getting parameter type metadata are not supported");
      Common.assertThrowsContains(
          SQLException.class, () -> parameterMetaData.isNullable(-1), "Wrong index position");
      Common.assertThrowsContains(
          SQLException.class, () -> parameterMetaData.isNullable(3), "Wrong index position");
    }
  }

  @Test
  public void parameterMetaDataNotPreparable() throws Exception {
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts=false")) {
      // statement that cannot be prepared
      try (PreparedStatement pstmt =
          con.prepareStatement("select  TMP.field1 from (select ? from dual) TMP")) {
        ParameterMetaData meta = pstmt.getParameterMetaData();
        assertEquals(1, meta.getParameterCount());
        assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(1));

        assertEquals(ParameterMetaData.parameterNullable, meta.isNullable(1));
        assertTrue(meta.isSigned(1));
        meta.unwrap(java.sql.ParameterMetaData.class);
        meta.isWrapperFor(java.sql.ParameterMetaData.class);
        Common.assertThrowsContains(
            SQLSyntaxErrorException.class,
            () -> meta.getParameterTypeName(1),
            "Unknown parameter metadata type name");
        Common.assertThrowsContains(
            SQLFeatureNotSupportedException.class,
            () -> meta.getParameterClassName(1),
            "Unknown parameter metadata class name");
        Common.assertThrowsContains(
            SQLFeatureNotSupportedException.class,
            () -> meta.getParameterType(1),
            "Getting parameter type metadata is not supported");
        Common.assertThrowsContains(
            SQLSyntaxErrorException.class,
            () -> meta.getPrecision(1),
            "Unknown parameter metadata precision");
        Common.assertThrowsContains(
            SQLSyntaxErrorException.class,
            () -> meta.getScale(1),
            "Unknown parameter metadata scale");

        Common.assertThrowsContains(
            SQLSyntaxErrorException.class,
            () -> meta.getParameterMode(0),
            "Wrong index position. Is 0 but must be in 1-1 range");
        Common.assertThrowsContains(
            SQLSyntaxErrorException.class,
            () -> meta.getParameterTypeName(0),
            "Wrong index position. Is 0 but must be in 1-1 range");
        Common.assertThrowsContains(
            SQLSyntaxErrorException.class,
            () -> meta.getParameterMode(0),
            "Wrong index position. Is 0 but must be in 1-1 range");
        Common.assertThrowsContains(
            SQLSyntaxErrorException.class,
            () -> meta.getParameterClassName(0),
            "Wrong index position. Is 0 but must be in 1-1 range");
        Common.assertThrowsContains(
            SQLSyntaxErrorException.class,
            () -> meta.getParameterType(2),
            "Wrong index position. Is 2 but must be in 1-1 range");
        Common.assertThrowsContains(
            SQLSyntaxErrorException.class,
            () -> meta.getPrecision(0),
            "Wrong index position. Is 0 but must be in 1-1 range");
        Common.assertThrowsContains(
            SQLSyntaxErrorException.class,
            () -> meta.getScale(0),
            "Wrong index position. Is 0 but must be in 1-1 range");
      }
    }
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts")) {
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
      if (!isMariaDBServer() && (minVersion(8, 0, 22) && !minVersion(8, 0, 30))) {
        assertEquals(16383, meta.getPrecision(1));
        assertEquals(31, meta.getScale(1));
      } else {
        // Xpand datatype as issue https://jira.mariadb.org/browse/XPT-279
        if (!isXpand()) {
          assertEquals(0, meta.getPrecision(1));
          assertEquals(0, meta.getScale(1));
        }
      }

      assertTrue(meta.isSigned(1));
      assertEquals(ParameterMetaData.parameterNullable, meta.isNullable(1));
      assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(1));

      Common.assertThrowsContains(
          SQLFeatureNotSupportedException.class,
          () -> meta.getParameterType(1),
          "Getting parameter type " + "metadata are not supported");
      Common.assertThrowsContains(
          SQLFeatureNotSupportedException.class,
          () -> meta.getParameterClassName(1),
          "Unknown parameter metadata class name");

      Common.assertThrowsContains(
          SQLException.class,
          () -> meta.getScale(20),
          "Wrong index position. Is 20 but must be in 1-3 range");
      Common.assertThrowsContains(
          SQLException.class,
          () -> meta.getPrecision(20),
          "Wrong index position. Is 20 but must be in 1-3 range");
      Common.assertThrowsContains(
          SQLException.class,
          () -> meta.isSigned(20),
          "Wrong index position. Is 20 but must be in 1-3 range");
      Common.assertThrowsContains(
          SQLException.class,
          () -> meta.isNullable(20),
          "Wrong index position. Is 20 but must be in 1-3 range");
      Common.assertThrowsContains(
          SQLException.class,
          () -> meta.getParameterTypeName(20),
          "Wrong index position. Is 20 but must be in 1-3 range");
      Common.assertThrowsContains(
          SQLException.class,
          () -> meta.getParameterMode(20),
          "Wrong index position. Is 20 but must be in 1-3 range");

      meta.unwrap(java.sql.ParameterMetaData.class);
      Common.assertThrowsContains(
          SQLException.class,
          () -> meta.unwrap(String.class),
          "The receiver is not a wrapper for java.lang.String");
    }
  }
}
