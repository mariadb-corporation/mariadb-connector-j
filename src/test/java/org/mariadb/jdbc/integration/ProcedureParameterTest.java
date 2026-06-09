// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.BitSet;
import java.util.Locale;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.Statement;

public class ProcedureParameterTest extends Common {

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE PROCEDURE useParameterName(a int) begin select a; end");
    stmt.execute(
        "CREATE PROCEDURE withStrangeParameter(IN a DECIMAL(10,2)) begin select a as b; end");
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
    // error MXS-3929 for maxscale 6.2.0
    Assumptions.assumeTrue(
        !sharedConn.getMetaData().getDatabaseProductVersion().contains("maxScale-6.2.0"));

    CallableStatement stmt = sharedConn.prepareCall("{call useParameterName(?)}");
    stmt.setInt("a", 1);
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    int res = rs.getInt(1);
    assertEquals(res, 1);
  }

  @Test
  public void callWithoutBracket() throws Exception {
    // error MXS-3929 for maxscale 6.2.0
    Assumptions.assumeTrue(
        !sharedConn.getMetaData().getDatabaseProductVersion().contains("maxScale-6.2.0"));

    CallableStatement stmt = sharedConn.prepareCall("call useParameterName(?)");
    stmt.setInt(1, 1);
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    int res = rs.getInt(1);
    assertEquals(res, 1);
  }

  @Test
  public void callWithStrangeParameter() throws SQLException {
    // error MXS-3929 for maxscale 6.2.0
    Assumptions.assumeTrue(
        !sharedConn.getMetaData().getDatabaseProductVersion().contains("maxScale-6.2.0"));

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
    basicProcedure(sharedConn);
    basicProcedure(sharedConnBinary);
  }

  private void basicProcedure(Connection con) throws SQLException {
    // error MXS-3929 for maxscale 6.2.0
    Assumptions.assumeTrue(
        !con.getMetaData().getDatabaseProductVersion().contains("maxScale-6.2.0"));

    Statement stmt = (Statement) con.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS basic_proc2");
    stmt.execute(
        "CREATE PROCEDURE basic_proc2 (INOUT t1 INT, IN t2 MEDIUMINT unsigned, OUT t3 DECIMAL(8,3),"
            + " OUT t4 VARCHAR(20), IN t5 SMALLINT, IN t6 BOOLEAN) BEGIN \n"
            + "set t3 = t1 * t5;\n"
            + "set t1 = t2 * t1;\n"
            + "set t4 = 'return data';\n"
            + "set t6 = true;\n"
            + "END");
    try (CallableStatement callableStatement = con.prepareCall("{call basic_proc2(?,?,?,?)}")) {
      ParameterMetaData meta = callableStatement.getParameterMetaData();
      callableStatement.getParameterMetaData();
      assertEquals(6, meta.getParameterCount());
      assertEquals("int", meta.getParameterClassName(1));
      assertEquals("int", meta.getParameterClassName(2));
      assertEquals("java.math.BigDecimal", meta.getParameterClassName(3));
      assertEquals("java.lang.String", meta.getParameterClassName(4));
      assertEquals("short", meta.getParameterClassName(5));
      assertEquals("boolean", meta.getParameterClassName(6));
      Common.assertThrowsContains(
          SQLException.class, () -> meta.getParameterClassName(7), "invalid parameter index 7");

      assertEquals("INT", meta.getParameterTypeName(1));
      assertEquals("MEDIUMINT", meta.getParameterTypeName(2));
      assertEquals("DECIMAL", meta.getParameterTypeName(3));
      assertEquals("VARCHAR", meta.getParameterTypeName(4));
      assertEquals("SMALLINT", meta.getParameterTypeName(5));
      assertEquals("BOOLEAN", meta.getParameterTypeName(6));
      Common.assertThrowsContains(
          SQLException.class, () -> meta.getParameterTypeName(0), "invalid parameter index 0");

      assertEquals(Types.INTEGER, meta.getParameterType(1));
      assertEquals(Types.INTEGER, meta.getParameterType(2));
      assertEquals(Types.DECIMAL, meta.getParameterType(3));
      assertEquals(Types.VARCHAR, meta.getParameterType(4));
      assertEquals(Types.SMALLINT, meta.getParameterType(5));
      assertEquals(Types.BOOLEAN, meta.getParameterType(6));
      Common.assertThrowsContains(
          SQLException.class, () -> meta.getParameterType(0), "invalid parameter index 0");

      assertEquals(ParameterMetaData.parameterModeInOut, meta.getParameterMode(1));
      assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(2));
      assertEquals(ParameterMetaData.parameterModeOut, meta.getParameterMode(3));
      assertEquals(ParameterMetaData.parameterModeOut, meta.getParameterMode(4));
      assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(5));
      assertEquals(ParameterMetaData.parameterModeIn, meta.getParameterMode(6));
      Common.assertThrowsContains(
          SQLException.class, () -> meta.getParameterMode(10), "invalid parameter index 10");

      assertEquals(10, meta.getPrecision(1));
      assertEquals(8, meta.getPrecision(3));
      assertEquals(20, meta.getPrecision(4));
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

      assertNotNull(meta.unwrap(org.mariadb.jdbc.CallableParameterMetaData.class));
      assertNotNull(meta.unwrap(ParameterMetaData.class));
      Common.assertThrowsContains(
          SQLException.class,
          () -> meta.unwrap(String.class),
          "The receiver is not a wrapper for java.lang.String");
      assertTrue(meta.isWrapperFor(org.mariadb.jdbc.CallableParameterMetaData.class));
      assertTrue(meta.isWrapperFor(ParameterMetaData.class));
      assertFalse(meta.isWrapperFor(String.class));
    }
    try (CallableStatement callableStatement = con.prepareCall("{call basic_proc2(?,?,?,?)}")) {
      ParameterMetaData meta = callableStatement.getParameterMetaData();
      callableStatement.getParameterMetaData();
      assertEquals(6, meta.getParameterCount());
      assertEquals("int", meta.getParameterClassName(1));
      assertEquals("int", meta.getParameterClassName(2));
      assertEquals("java.math.BigDecimal", meta.getParameterClassName(3));
      assertEquals("java.lang.String", meta.getParameterClassName(4));
      assertEquals("short", meta.getParameterClassName(5));
      assertEquals("boolean", meta.getParameterClassName(6));
      Common.assertThrowsContains(
          SQLException.class, () -> meta.getParameterClassName(7), "invalid parameter index 7");
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

  @Test
  public void parameterMetaDataAcrossLookupPaths() throws SQLException {
    Assumptions.assumeTrue(
        !sharedConn.getMetaData().getDatabaseProductVersion().contains("maxScale-6.2.0"));

    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS allTypesProc");
    stmt.execute("DROP FUNCTION IF EXISTS allTypesFunc");
    stmt.execute(createAllTypesProcSql());
    stmt.execute(createAllTypesFuncSql());

    // Probe whether mysql.proc is readable for the test user (dropped in MySQL 8.0+, often
    // permission-restricted on shared MariaDB hosts).
    boolean mysqlProcReadable;
    try {
      stmt.executeQuery("SELECT name FROM mysql.proc WHERE 1=0").close();
      mysqlProcReadable = true;
    } catch (SQLException ignore) {
      mysqlProcReadable = false;
    }

    if (mysqlProcReadable) {
      try (Connection con = createCon()) {
        HostAddress host = con.getClient().getHostAddress();
        host.setMysqlProcStatus(HostAddress.MysqlProcStatus.UNKNOWN);
        assertAllTypesProcMeta(con);
        assertAllTypesFuncMeta(con);
        // After at least one successful fast-path call, status should latch USABLE.
        assertEquals(HostAddress.MysqlProcStatus.USABLE, host.getMysqlProcStatus());
      }
    }

    try (Connection con = createCon()) {
      HostAddress host = con.getClient().getHostAddress();
      host.setMysqlProcStatus(HostAddress.MysqlProcStatus.UNUSABLE);
      assertAllTypesProcMeta(con);
      assertAllTypesFuncMeta(con);
      // Status must stay UNUSABLE — the fast path is bypassed entirely.
      assertEquals(HostAddress.MysqlProcStatus.UNUSABLE, host.getMysqlProcStatus());
    }

    stmt.execute("DROP PROCEDURE IF EXISTS allTypesProc");
    stmt.execute("DROP FUNCTION IF EXISTS allTypesFunc");
  }

  @Test
  public void parameterMetaDataOverUnixSocket() throws SQLException {
    Assumptions.assumeTrue(
        System.getenv("local") != null
            && "1".equals(System.getenv("local"))
            && !System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win"));
    Assumptions.assumeTrue(
        !sharedConn.getMetaData().getDatabaseProductVersion().contains("maxScale-6.2.0"));

    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("select @@version_compile_os,@@socket");
    if (!rs.next() || rs.getString(2) == null) {
      return;
    }
    String socketPath = rs.getString(2);

    stmt.execute("DROP PROCEDURE IF EXISTS allTypesProc");
    stmt.execute("DROP FUNCTION IF EXISTS allTypesFunc");
    stmt.execute(createAllTypesProcSql());
    stmt.execute(createAllTypesFuncSql());

    // Hostless URL: empty host between // and /, only localSocket option to connect.
    String url =
        "jdbc:mariadb:///"
            + database
            + "?localSocket="
            + socketPath
            + "&user="
            + user
            + (password == null || password.isEmpty() ? "" : "&password=" + password);

    try (Connection con = (Connection) DriverManager.getConnection(url)) {
      HostAddress host = con.getClient().getHostAddress();
      assertNull(host.host, "expected hostless HostAddress for jdbc:mariadb:/// URL");
      assertEquals(socketPath, host.localSocket);

      // Force fast path probe.
      host.setMysqlProcStatus(HostAddress.MysqlProcStatus.UNKNOWN);
      assertAllTypesProcMeta(con);
      assertAllTypesFuncMeta(con);
      HostAddress.MysqlProcStatus afterProbe = host.getMysqlProcStatus();
      assertTrue(
          afterProbe == HostAddress.MysqlProcStatus.USABLE
              || afterProbe == HostAddress.MysqlProcStatus.UNUSABLE,
          "expected status to latch after probe, got " + afterProbe);

      // Force the information_schema fallback explicitly.
      host.setMysqlProcStatus(HostAddress.MysqlProcStatus.UNUSABLE);
      assertAllTypesProcMeta(con);
      assertAllTypesFuncMeta(con);
    }

    stmt.execute("DROP PROCEDURE IF EXISTS allTypesProc");
    stmt.execute("DROP FUNCTION IF EXISTS allTypesFunc");
  }

  private static String createAllTypesProcSql() {
    return "CREATE PROCEDURE allTypesProc(\n"
        + "  INOUT p1 INT,\n"
        + "  IN p2 MEDIUMINT UNSIGNED,\n"
        + "  OUT p3 DECIMAL(10,3),\n"
        + "  OUT p4 VARCHAR(25),\n"
        + "  IN p5 SMALLINT,\n"
        + "  IN p6 BOOLEAN,\n"
        + "  IN p7 TINYINT,\n"
        + "  IN p8 BIGINT,\n"
        + "  IN p9 FLOAT,\n"
        + "  IN p10 DOUBLE,\n"
        + "  IN p11 NUMERIC(5,2),\n"
        + "  IN p12 INTEGER,\n"
        + "  IN p13 BIT(8),\n"
        + "  IN p14 CHAR(6),\n"
        + "  IN p15 BINARY(4),\n"
        + "  IN p16 VARBINARY(7),\n"
        + "  IN p17 DATE,\n"
        + "  IN p18 TIME,\n"
        + "  IN p19 TIMESTAMP,\n"
        + "  IN p20 DATETIME,\n"
        + "  IN p21 YEAR\n"
        + ") BEGIN SELECT 1; END";
  }

  private static String createAllTypesFuncSql() {
    return "CREATE FUNCTION allTypesFunc(p1 INT, p2 VARCHAR(10)) RETURNS DECIMAL(8,2)"
        + " DETERMINISTIC RETURN 1";
  }

  private static void assertAllTypesProcMeta(Connection con) throws SQLException {
    try (CallableStatement cs =
        con.prepareCall("{call allTypesProc(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}")) {
      org.mariadb.jdbc.CallableParameterMetaData meta =
          cs.getParameterMetaData().unwrap(org.mariadb.jdbc.CallableParameterMetaData.class);
      assertEquals(21, meta.getParameterCount());

      // index, name, mode, sqlType, typeName, className, precision, scale, signed
      assertParam(
          meta,
          1,
          "p1",
          ParameterMetaData.parameterModeInOut,
          Types.INTEGER,
          "INT",
          int.class.getName(),
          10,
          0,
          true);
      assertParam(
          meta,
          2,
          "p2",
          ParameterMetaData.parameterModeIn,
          Types.INTEGER,
          "MEDIUMINT",
          int.class.getName(),
          8,
          0,
          false);
      assertParam(
          meta,
          3,
          "p3",
          ParameterMetaData.parameterModeOut,
          Types.DECIMAL,
          "DECIMAL",
          BigDecimal.class.getName(),
          10,
          3,
          true);
      assertParam(
          meta,
          4,
          "p4",
          ParameterMetaData.parameterModeOut,
          Types.VARCHAR,
          "VARCHAR",
          String.class.getName(),
          25,
          0,
          true);
      assertParam(
          meta,
          5,
          "p5",
          ParameterMetaData.parameterModeIn,
          Types.SMALLINT,
          "SMALLINT",
          short.class.getName(),
          5,
          0,
          true);
      // BOOLEAN must be normalized to TINYINT(1) on both paths.
      assertParam(
          meta,
          6,
          "p6",
          ParameterMetaData.parameterModeIn,
          Types.BOOLEAN,
          "BOOLEAN",
          boolean.class.getName(),
          3,
          0,
          true);
      assertParam(
          meta,
          7,
          "p7",
          ParameterMetaData.parameterModeIn,
          Types.TINYINT,
          "TINYINT",
          byte.class.getName(),
          3,
          0,
          true);
      assertParam(
          meta,
          8,
          "p8",
          ParameterMetaData.parameterModeIn,
          Types.BIGINT,
          "BIGINT",
          long.class.getName(),
          19,
          0,
          true);
      assertParam(
          meta,
          9,
          "p9",
          ParameterMetaData.parameterModeIn,
          Types.FLOAT,
          "FLOAT",
          float.class.getName(),
          12,
          0,
          true);
      assertParam(
          meta,
          10,
          "p10",
          ParameterMetaData.parameterModeIn,
          Types.DOUBLE,
          "DOUBLE",
          double.class.getName(),
          22,
          0,
          true);
      // NUMERIC must be normalized to DECIMAL on both paths.
      assertParam(
          meta,
          11,
          "p11",
          ParameterMetaData.parameterModeIn,
          Types.DECIMAL,
          "DECIMAL",
          BigDecimal.class.getName(),
          5,
          2,
          true);
      // INTEGER alias must be normalized to INT on both paths.
      assertParam(
          meta,
          12,
          "p12",
          ParameterMetaData.parameterModeIn,
          Types.INTEGER,
          "INT",
          int.class.getName(),
          10,
          0,
          true);
      assertParam(
          meta,
          13,
          "p13",
          ParameterMetaData.parameterModeIn,
          Types.BIT,
          "BIT",
          BitSet.class.getName(),
          8,
          0,
          true);
      assertParam(
          meta,
          14,
          "p14",
          ParameterMetaData.parameterModeIn,
          Types.CHAR,
          "CHAR",
          String.class.getName(),
          6,
          0,
          true);
      assertParam(
          meta,
          15,
          "p15",
          ParameterMetaData.parameterModeIn,
          Types.BINARY,
          "BINARY",
          byte[].class.getName(),
          4,
          0,
          true);
      assertParam(
          meta,
          16,
          "p16",
          ParameterMetaData.parameterModeIn,
          Types.VARBINARY,
          "VARBINARY",
          byte[].class.getName(),
          7,
          0,
          true);
      assertParam(
          meta,
          17,
          "p17",
          ParameterMetaData.parameterModeIn,
          Types.DATE,
          "DATE",
          Date.class.getName(),
          0,
          0,
          true);
      assertParam(
          meta,
          18,
          "p18",
          ParameterMetaData.parameterModeIn,
          Types.TIME,
          "TIME",
          Time.class.getName(),
          0,
          0,
          true);
      assertParam(
          meta,
          19,
          "p19",
          ParameterMetaData.parameterModeIn,
          Types.TIMESTAMP,
          "TIMESTAMP",
          Timestamp.class.getName(),
          0,
          0,
          true);
      assertParam(
          meta,
          20,
          "p20",
          ParameterMetaData.parameterModeIn,
          Types.TIMESTAMP,
          "DATETIME",
          Timestamp.class.getName(),
          0,
          0,
          true);
      assertParam(
          meta,
          21,
          "p21",
          ParameterMetaData.parameterModeIn,
          Types.SMALLINT,
          "YEAR",
          short.class.getName(),
          0,
          0,
          true);

      // Nullable is always parameterNullableUnknown — exercise once per metadata.
      assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
      // Out-of-range indexes must throw with a descriptive message.
      assertThrows(SQLException.class, () -> meta.getParameterType(0));
      assertThrows(SQLException.class, () -> meta.getParameterType(22));
    }
  }

  private static void assertAllTypesFuncMeta(Connection con) throws SQLException {
    try (CallableStatement cs = con.prepareCall("{? = call allTypesFunc(?,?)}")) {
      org.mariadb.jdbc.CallableParameterMetaData meta =
          cs.getParameterMetaData().unwrap(org.mariadb.jdbc.CallableParameterMetaData.class);
      assertEquals(3, meta.getParameterCount());

      // Return value — every parameter on a function is reported as OUT.
      assertEquals(ParameterMetaData.parameterModeOut, meta.getParameterMode(1));
      assertEquals(Types.DECIMAL, meta.getParameterType(1));
      assertEquals("DECIMAL", meta.getParameterTypeName(1));
      assertEquals(BigDecimal.class.getName(), meta.getParameterClassName(1));
      assertEquals(8, meta.getPrecision(1));
      assertEquals(2, meta.getScale(1));

      // First declared parameter
      assertEquals(ParameterMetaData.parameterModeOut, meta.getParameterMode(2));
      assertEquals(Types.INTEGER, meta.getParameterType(2));
      assertEquals("INT", meta.getParameterTypeName(2));
      assertEquals(10, meta.getPrecision(2));

      // Second declared parameter
      assertEquals(ParameterMetaData.parameterModeOut, meta.getParameterMode(3));
      assertEquals(Types.VARCHAR, meta.getParameterType(3));
      assertEquals("VARCHAR", meta.getParameterTypeName(3));
      assertEquals(String.class.getName(), meta.getParameterClassName(3));
      assertEquals(10, meta.getPrecision(3));
    }
  }

  private static void assertParam(
      org.mariadb.jdbc.CallableParameterMetaData meta,
      int idx,
      String name,
      int mode,
      int sqlType,
      String typeName,
      String className,
      int precision,
      int scale,
      boolean signed)
      throws SQLException {
    String tag = "param " + idx + " (" + name + ")";
    // PARAMETER_NAME is only reliable on the mysql.proc fast path; information_schema may strip
    // backticks / change case. Compare case-insensitively when present.
    String actualName = meta.getParameterName(idx);
    if (actualName != null) {
      assertEquals(
          name.toLowerCase(java.util.Locale.ROOT),
          actualName.toLowerCase(java.util.Locale.ROOT),
          tag + " name");
    }
    assertEquals(mode, meta.getParameterMode(idx), tag + " mode");
    assertEquals(sqlType, meta.getParameterType(idx), tag + " sqlType");
    assertEquals(typeName, meta.getParameterTypeName(idx), tag + " typeName");
    assertEquals(className, meta.getParameterClassName(idx), tag + " className");
    assertEquals(precision, meta.getPrecision(idx), tag + " precision");
    assertEquals(scale, meta.getScale(idx), tag + " scale");
    assertEquals(signed, meta.isSigned(idx), tag + " signed");
  }

  @Test
  public void failStoredProcedureTest() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS workingStoreProcedure");
    stmt.execute("DROP PROCEDURE IF EXISTS failingStoreProcedure");
    stmt.execute("CREATE PROCEDURE workingStoreProcedure(a int) begin Do 1; end");
    stmt.execute(
        "CREATE PROCEDURE failingStoreProcedure(a int) begin SIGNAL SQLSTATE '45000' SET"
            + " MESSAGE_TEXT = 'Custom error'; end");

    try (CallableStatement call1 = sharedConn.prepareCall("{call useParameterName(?)}")) {
      call1.setInt(1, 1);
      call1.execute();

      try (CallableStatement call2 = sharedConn.prepareCall("{call failingStoreProcedure(?)}")) {
        call2.setInt(1, 1);
        assertThrows(SQLException.class, () -> call2.execute());

        call2.setInt(1, 1);
        call2.addBatch();
        call2.setInt(1, 2);
        call2.addBatch();
        assertThrows(SQLException.class, () -> call2.executeBatch());

        call1.setInt(1, 1);
        call1.addBatch();
        call1.setInt(1, 2);
        call1.addBatch();
        call1.executeBatch();
      }
    }
  }

  @Test
  public void failStoredProcedureTest2() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS saveDataProc");
    stmt.execute("DROP TABLE IF EXISTS workingStoreProcedure");
    stmt.execute("CREATE TABLE workingStoreProcedure(i int PRIMARY KEY)");
    stmt.execute(
        "CREATE PROCEDURE saveDataProc(val int) begin INSERT INTO workingStoreProcedure(i) VALUE"
            + " (val); end");

    try (CallableStatement call1 = sharedConn.prepareCall("{call saveDataProc(?)}")) {
      call1.setInt(1, 1);
      call1.execute();

      call1.setInt(1, 1);
      assertThrows(SQLException.class, () -> call1.execute());

      call1.setInt(1, 2);
      call1.addBatch();
      call1.setInt(1, 3);
      call1.addBatch();
      call1.executeBatch();

      call1.setInt(1, 2);
      call1.addBatch();
      call1.setInt(1, 4);
      call1.addBatch();
      assertThrows(SQLException.class, () -> call1.executeBatch());

      call1.setInt(1, 5);
      call1.addBatch();
      call1.setInt(1, 6);
      call1.addBatch();
      call1.executeBatch();
    }
  }
}
