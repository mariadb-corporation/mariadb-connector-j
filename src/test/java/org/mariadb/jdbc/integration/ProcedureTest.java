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

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.MariaDbClob;
import org.mariadb.jdbc.Statement;

public class ProcedureTest extends Common {

  @Test
  public void wrongCall() throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> sharedConn.prepareCall("SELECT ?"), "invalid callable syntax");
  }

  @Test
  public void prep() throws SQLException {
    Statement st = sharedConn.createStatement();
    st.execute("DROP PROCEDURE IF EXISTS prep_proc");
    st.execute("CREATE PROCEDURE prep_proc (IN t1 INT) BEGIN \n" + "SELECT t1;\n" + "END");

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
  public void basicProcedure() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS basic_proc");
    stmt.execute(
        "CREATE PROCEDURE basic_proc (IN t1 INT, INOUT t2 INT unsigned, OUT t3 INT, IN t4 INT) BEGIN \n"
            + "set t3 = t1 * t4;\n"
            + "set t2 = t2 * t1;\n"
            + "END");
    try (CallableStatement callableStatement =
        sharedConn.prepareCall("{call basic_proc(?,?,?,?)}")) {
      assertThrowsContains(
          SQLException.class, () -> callableStatement.getString(1), "No output result");
      callableStatement.getParameterMetaData();
      assertThrowsContains(
          SQLSyntaxErrorException.class,
          () -> callableStatement.registerOutParameter(20, JDBCType.INTEGER),
          "wrong parameter index 20");

      callableStatement.registerOutParameter(2, JDBCType.INTEGER);
      callableStatement.registerOutParameter(3, JDBCType.INTEGER);
      checkResults(callableStatement);

      callableStatement.registerOutParameter(2, Types.INTEGER);
      callableStatement.registerOutParameter(3, Types.INTEGER);
      checkResults(callableStatement);

      callableStatement.registerOutParameter(2, Types.INTEGER, 10);
      callableStatement.registerOutParameter(3, Types.INTEGER, 10);
      checkResults(callableStatement);

      callableStatement.registerOutParameter(2, JDBCType.INTEGER, 10);
      callableStatement.registerOutParameter(3, JDBCType.INTEGER, 10);
      checkResults(callableStatement);

      callableStatement.registerOutParameter(2, Types.INTEGER, "typeName");
      callableStatement.registerOutParameter(3, Types.INTEGER, "typeName");
      checkResults(callableStatement);

      callableStatement.registerOutParameter(2, JDBCType.INTEGER, "typeName");
      callableStatement.registerOutParameter(3, JDBCType.INTEGER, "typeName");
      checkResults(callableStatement);

      callableStatement.registerOutParameter("t2", JDBCType.INTEGER);
      callableStatement.registerOutParameter("t3", JDBCType.INTEGER);
      checkResults(callableStatement);

      callableStatement.registerOutParameter("t2", Types.INTEGER);
      callableStatement.registerOutParameter("t3", Types.INTEGER);
      checkResults(callableStatement);

      callableStatement.registerOutParameter("t2", Types.INTEGER, 10);
      callableStatement.registerOutParameter("t3", Types.INTEGER, 10);
      checkResults(callableStatement);

      callableStatement.registerOutParameter("t2", JDBCType.INTEGER, 10);
      callableStatement.registerOutParameter("t3", JDBCType.INTEGER, 10);
      checkResults(callableStatement);

      callableStatement.registerOutParameter("t2", Types.INTEGER, "typeName");
      callableStatement.registerOutParameter("t3", Types.INTEGER, "typeName");
      checkResults(callableStatement);

      callableStatement.registerOutParameter("t2", JDBCType.INTEGER, "typeName");
      callableStatement.registerOutParameter("t3", JDBCType.INTEGER, "typeName");
      checkResults(callableStatement);

      assertThrowsContains(
          SQLException.class,
          () -> callableStatement.registerOutParameter(100, JDBCType.BINARY),
          "wrong parameter index 100");
      assertThrowsContains(
          SQLException.class,
          () -> callableStatement.registerOutParameter("unknown", JDBCType.INTEGER),
          "parameterName unknown not found");
    }
  }

  @Test
  @SuppressWarnings("deprecated")
  public void multiOutputProcedure() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS multiOutputProcedure");
    stmt.execute(
        "CREATE PROCEDURE multiOutputProcedure (IN t1 INT, INOUT t2 INT unsigned, OUT t3 INT, IN t4 INT) BEGIN \n"
            + "SELECT 1;"
            + "set t3 = t1 * t4;\n"
            + "set t2 = t2 * t1;\n"
            + "END");
    try (CallableStatement callableStatement =
        sharedConn.prepareCall("{call multiOutputProcedure(?,?,?,?)}")) {
      assertThrowsContains(
          SQLException.class, () -> callableStatement.getString(1), "No output result");
      callableStatement.getParameterMetaData();
      assertThrowsContains(
          SQLSyntaxErrorException.class,
          () -> callableStatement.registerOutParameter(20, JDBCType.INTEGER),
          "wrong parameter index 20");

      callableStatement.registerOutParameter(2, JDBCType.INTEGER);
      callableStatement.registerOutParameter(3, JDBCType.INTEGER);
      checkResults(callableStatement);

      callableStatement.registerOutParameter(2, Types.INTEGER);
      callableStatement.registerOutParameter(3, Types.INTEGER);
      checkResults(callableStatement);

      callableStatement.registerOutParameter(2, Types.INTEGER, 10);
      callableStatement.registerOutParameter(3, Types.INTEGER, 10);
      checkResults(callableStatement);

      callableStatement.registerOutParameter(2, JDBCType.INTEGER, 10);
      callableStatement.registerOutParameter(3, JDBCType.INTEGER, 10);
      checkResults(callableStatement);

      callableStatement.registerOutParameter(2, Types.INTEGER, "typeName");
      callableStatement.registerOutParameter(3, Types.INTEGER, "typeName");
      checkResults(callableStatement);

      callableStatement.registerOutParameter(2, JDBCType.INTEGER, "typeName");
      callableStatement.registerOutParameter(3, JDBCType.INTEGER, "typeName");
      checkResults(callableStatement);

      callableStatement.registerOutParameter("t2", JDBCType.INTEGER);
      callableStatement.registerOutParameter("t3", JDBCType.INTEGER);
      checkResults(callableStatement);

      callableStatement.registerOutParameter("t2", Types.INTEGER);
      callableStatement.registerOutParameter("t3", Types.INTEGER);
      checkResults(callableStatement);

      callableStatement.registerOutParameter("t2", Types.INTEGER, 10);
      callableStatement.registerOutParameter("t3", Types.INTEGER, 10);
      checkResults(callableStatement);

      callableStatement.registerOutParameter("t2", JDBCType.INTEGER, 10);
      callableStatement.registerOutParameter("t3", JDBCType.INTEGER, 10);
      checkResults(callableStatement);

      callableStatement.registerOutParameter("t2", Types.INTEGER, "typeName");
      callableStatement.registerOutParameter("t3", Types.INTEGER, "typeName");
      checkResults(callableStatement);

      callableStatement.registerOutParameter("t2", JDBCType.INTEGER, "typeName");
      callableStatement.registerOutParameter("t3", JDBCType.INTEGER, "typeName");
      checkResults(callableStatement);

      assertThrowsContains(
          SQLException.class,
          () -> callableStatement.registerOutParameter(100, JDBCType.BINARY),
          "wrong parameter index 100");
      assertThrowsContains(
          SQLException.class,
          () -> callableStatement.registerOutParameter("unknown", JDBCType.INTEGER),
          "parameterName unknown not found");
    }
  }

  @SuppressWarnings("deprecation")
  private void checkResults(CallableStatement callableStatement) throws SQLException {
    callableStatement.setInt(1, 2);
    callableStatement.setInt(2, 3);
    callableStatement.setInt(4, 10);
    callableStatement.execute();

    assertEquals(6, callableStatement.getInt(2));
    assertEquals("6", callableStatement.getString(2));
    assertTrue(callableStatement.getBoolean(2));
    assertEquals((byte) 6, callableStatement.getByte(2));
    assertEquals((short) 6, callableStatement.getShort(2));
    assertEquals(6L, callableStatement.getLong(2));
    assertEquals(6F, callableStatement.getFloat(2));
    assertEquals(6D, callableStatement.getDouble(2));
    assertEquals(6, callableStatement.getBigDecimal(2).intValue());
    assertEquals(6, callableStatement.getBigDecimal(2, 5).intValue());
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getDate(2),
        "Data type INTEGER cannot be decoded as Date");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getBytes(2),
        "Data type INTEGER cannot be decoded as byte[]");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getDate(2, (Calendar) null),
        "Data type INTEGER cannot be decoded as Date");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getTime(2),
        "Data type INTEGER cannot be decoded as Time");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getTime(2, (Calendar) null),
        "Data type INTEGER cannot be decoded as Time");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getTimestamp(2),
        "Data type INTEGER cannot be decoded as Timestamp");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getTimestamp(2, (Calendar) null),
        "Data type INTEGER cannot be decoded as Timestamp");
    assertEquals(6L, callableStatement.getObject(2));
    assertThrowsContains(
        SQLFeatureNotSupportedException.class,
        () -> callableStatement.getObject(2, (Map<String, Class<?>>) null),
        " Method ResultSet.getObject(int columnIndex, Map<String, Class<?>> map) not supported");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getRef(2),
        "Method ResultSet.getRef not supported");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getBlob(2),
        "Data type INTEGER cannot be decoded as Blob");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getClob(2),
        "Data type INTEGER cannot be decoded as Clob");
    assertThrowsContains(
        SQLFeatureNotSupportedException.class,
        () -> callableStatement.getArray(2),
        "Method ResultSet.getArray not supported");
    assertThrowsContains(
        SQLException.class, () -> callableStatement.getURL(2), "Could not parse '6' as URL");

    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getNClob(2),
        "Data type INTEGER cannot be decoded as Clob");
    assertThrowsContains(
        SQLFeatureNotSupportedException.class,
        () -> callableStatement.getSQLXML(2),
        "SQLXML are not supported");
    assertEquals("6", callableStatement.getNString(2));
    assertThrowsContains(
        SQLDataException.class,
        () -> callableStatement.getNCharacterStream(2),
        "Data type INTEGER cannot be decoded as Reader");
    assertThrowsContains(
        SQLDataException.class,
        () -> callableStatement.getCharacterStream(2),
        "Data type INTEGER cannot be decoded as Reader");
    assertEquals("6", callableStatement.getObject(2, String.class));
    assertThrowsContains(
        SQLException.class, () -> callableStatement.getRowId(2), "RowId are not supported");

    assertEquals(6, callableStatement.getInt("t2"));
    assertEquals("6", callableStatement.getString("t2"));
    assertTrue(callableStatement.getBoolean("t2"));
    assertEquals((byte) 6, callableStatement.getByte("t2"));
    assertEquals((short) 6, callableStatement.getShort("t2"));
    assertEquals(6L, callableStatement.getLong("t2"));
    assertEquals(6F, callableStatement.getFloat("t2"));
    assertEquals(6D, callableStatement.getDouble("t2"));
    assertEquals(6, callableStatement.getBigDecimal("t2").intValue());
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getBytes("t2"),
        "Data type INTEGER cannot be decoded as byte[]");
    assertThrowsContains(
        SQLException.class, () -> callableStatement.getRowId("t2"), "RowId are not supported");

    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getDate("t2"),
        "Data type INTEGER cannot be decoded as Date");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getDate("t2", (Calendar) null),
        "Data type INTEGER cannot be decoded as Date");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getTime("t2"),
        "Data type INTEGER cannot be decoded as Time");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getTime("t2", (Calendar) null),
        "Data type INTEGER cannot be decoded as Time");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getTimestamp("t2"),
        "Data type INTEGER cannot be decoded as Timestamp");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getTimestamp("t2", (Calendar) null),
        "Data type INTEGER cannot be decoded as Timestamp");
    assertEquals(6L, callableStatement.getObject("t2"));
    assertThrowsContains(
        SQLFeatureNotSupportedException.class,
        () -> callableStatement.getObject("t2", (Map<String, Class<?>>) null),
        " Method ResultSet.getObject(int columnIndex, Map<String, Class<?>> map) not supported");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getRef("t2"),
        "Method ResultSet.getRef not supported");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getBlob("t2"),
        "Data type INTEGER cannot be decoded as Blob");
    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getClob("t2"),
        "Data type INTEGER cannot be decoded as Clob");
    assertThrowsContains(
        SQLFeatureNotSupportedException.class,
        () -> callableStatement.getArray("t2"),
        "Method ResultSet.getArray not supported");
    assertThrowsContains(
        SQLException.class, () -> callableStatement.getURL("t2"), "Could not parse '6' as URL");

    assertThrowsContains(
        SQLException.class,
        () -> callableStatement.getNClob("t2"),
        "Data type INTEGER cannot be decoded as Clob");
    assertThrowsContains(
        SQLFeatureNotSupportedException.class,
        () -> callableStatement.getSQLXML("t2"),
        "SQLXML are not supported");
    assertEquals("6", callableStatement.getNString("t2"));
    assertThrowsContains(
        SQLDataException.class,
        () -> callableStatement.getNCharacterStream("t2"),
        "Data type INTEGER cannot be decoded as Reader");
    assertThrowsContains(
        SQLDataException.class,
        () -> callableStatement.getCharacterStream("t2"),
        "Data type INTEGER cannot be decoded as Reader");
    assertEquals("6", callableStatement.getObject("t2", String.class));

    assertFalse(callableStatement.wasNull());
    assertEquals(20, callableStatement.getInt(3));
    assertFalse(callableStatement.wasNull());

    assertThrowsContains(
        SQLException.class, () -> callableStatement.getInt(4), "index 4 not declared as output");
  }

  @Test
  public void setProcedureTest() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS basic_proc");
    stmt.execute(
        "CREATE PROCEDURE basic_proc (IN t1 VARCHAR(20), INOUT t2 VARCHAR(20)) BEGIN \n"
            + "set t2 = CONCAT(t1, t2);\n"
            + "END");
    try (CallableStatement callableStatement = sharedConn.prepareCall("{call basic_proc(?,?)}")) {
      callableStatement.registerOutParameter(2, JDBCType.VARCHAR);
      callableStatement.setString(1, "a");
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("ab", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNString(1, "a");
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("ab", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setURL(1, new URL("http://a"));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("http://ab", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNull(1, Types.VARCHAR);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertNull(callableStatement.getString(2));
      assertTrue(callableStatement.wasNull());

      reset(callableStatement);
      callableStatement.setNull(1, Types.VARCHAR, "sometype");
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertNull(callableStatement.getString(2));
      assertTrue(callableStatement.wasNull());

      reset(callableStatement);
      callableStatement.setBoolean(1, true);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("1b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setByte(1, (byte) 'a');
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("97b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setShort(1, (short) 6);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("6b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setInt(1, 6);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("6b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setLong(1, 6L);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("6b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setFloat(1, 6F);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("6b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setDouble(1, 6D);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("6b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setBigDecimal(1, BigDecimal.ONE);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("1b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setTimestamp(1, new Timestamp(0));
      callableStatement.setString(2, "");
      callableStatement.execute();
      callableStatement.getTimestamp(2);

      reset(callableStatement);
      callableStatement.setTimestamp(1, new Timestamp(0), Calendar.getInstance());
      callableStatement.setString(2, "");
      callableStatement.execute();
      callableStatement.getTimestamp(2);

      reset(callableStatement);
      callableStatement.setBytes(1, new byte[] {(byte) 'a', (byte) 0x45});
      callableStatement.setString(2, "c");
      callableStatement.execute();
      assertEquals("aEc", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setBlob(1, new MariaDbBlob("0123".getBytes(), 1, 2));
      callableStatement.setString(2, "c");
      callableStatement.execute();
      assertEquals("12c", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setBlob(1, new ByteArrayInputStream("eef".getBytes()));
      callableStatement.setString(2, "c");
      callableStatement.execute();
      assertEquals("eefc", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setDate(1, new Date(0));
      callableStatement.setString(2, "");
      callableStatement.execute();
      callableStatement.getDate(2);

      reset(callableStatement);
      callableStatement.setDate(1, new Date(0), Calendar.getInstance());
      callableStatement.setString(2, "");
      callableStatement.execute();
      callableStatement.getDate(2, Calendar.getInstance());

      reset(callableStatement);
      callableStatement.setTime(1, new Time(0));
      callableStatement.setString(2, "");
      callableStatement.execute();
      callableStatement.getTime(2);

      reset(callableStatement);
      callableStatement.setTime(1, new Time(0), Calendar.getInstance());
      callableStatement.setString(2, "");
      callableStatement.execute();
      callableStatement.getTime(2);

      reset(callableStatement);
      callableStatement.setAsciiStream(1, new ByteArrayInputStream("dbc".getBytes()));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("dbcb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setAsciiStream(1, new ByteArrayInputStream("cbc".getBytes()), 2);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("cbb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setBinaryStream(1, new ByteArrayInputStream("bef".getBytes()));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("befb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setBinaryStream(1, new ByteArrayInputStream("aef".getBytes()), 2);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("aeb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setObject(1, new ByteArrayInputStream("saf".getBytes()));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("safb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setObject(
          1, new ByteArrayInputStream("sbf".getBytes()), Types.LONGVARBINARY);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sbfb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setObject(
          1, new ByteArrayInputStream("scf".getBytes()), JDBCType.LONGVARBINARY);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("scfb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setObject(1, null);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertNull(callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setObject(
          1, new ByteArrayInputStream("sdf".getBytes()), Types.LONGVARBINARY, 10);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sdfb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setObject(
          1, new ByteArrayInputStream("sea".getBytes()), JDBCType.LONGVARBINARY, 10);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("seab", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setCharacterStream(1, new StringReader("seb"));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sebb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setClob(1, new MariaDbClob("sec".getBytes(), 0, 16));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("secb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setClob(1, new StringReader("sed"));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sedb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setClob(1, new StringReader("sef"), 2);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("seb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNClob(1, new MariaDbClob("seg".getBytes(), 0, 16));
      callableStatement.setString(2, "c");
      callableStatement.execute();
      assertEquals("segc", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNClob(1, new StringReader("seh"));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sehb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNClob(1, new StringReader("sei"), 2);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("seb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setCharacterStream(1, new StringReader("sej"), 2);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("seb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNCharacterStream(1, new StringReader("seh"));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sehb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNCharacterStream(1, new StringReader("sek"), 2);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("seb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setDate(1, new Date(0));
      callableStatement.setString(2, "");
      callableStatement.execute();
      callableStatement.getDate(2);

      reset(callableStatement);
      callableStatement.setDate(1, new Date(0), Calendar.getInstance());
      callableStatement.setString(2, "");
      callableStatement.execute();
      callableStatement.getDate(2, Calendar.getInstance());

      reset(callableStatement);
      callableStatement.setNull(1, Types.BINARY);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertNull(callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNull(1, Types.BINARY, "ttt");
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertNull(callableStatement.getString(2));

      // with named params

      callableStatement.registerOutParameter(2, JDBCType.VARCHAR);
      callableStatement.setString("t1", "a");
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("ab", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNString("t1", "a");
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("ab", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setURL("t1", new URL("http://a"));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("http://ab", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNull("t1", Types.VARCHAR);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertNull(callableStatement.getString(2));
      assertTrue(callableStatement.wasNull());

      reset(callableStatement);
      callableStatement.setNull("t1", Types.VARCHAR, "sometype");
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertNull(callableStatement.getString(2));
      assertTrue(callableStatement.wasNull());

      reset(callableStatement);
      callableStatement.setBoolean("t1", true);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("1b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setByte("t1", (byte) 'a');
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("97b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setShort("t1", (short) 6);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("6b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setInt("t1", 6);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("6b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setLong("t1", 6L);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("6b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setFloat("t1", 6F);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("6b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setDouble("t1", 6D);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("6b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setBigDecimal("t1", BigDecimal.ONE);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("1b", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setTimestamp("t1", new Timestamp(0));
      callableStatement.setString(2, "");
      callableStatement.execute();
      callableStatement.getTimestamp("t2");

      reset(callableStatement);
      callableStatement.setTimestamp("t1", new Timestamp(0), Calendar.getInstance());
      callableStatement.setString(2, "");
      callableStatement.execute();
      callableStatement.getTimestamp("t2", Calendar.getInstance());

      reset(callableStatement);
      callableStatement.setBytes("t1", new byte[] {(byte) 'a', (byte) 0x45});
      callableStatement.setString(2, "c");
      callableStatement.execute();
      assertEquals("aEc", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setBlob("t1", new MariaDbBlob("0123".getBytes(), 1, 2));
      callableStatement.setString(2, "c");
      callableStatement.execute();
      assertEquals("12c", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setBlob("t1", new ByteArrayInputStream("sef".getBytes()));
      callableStatement.setString(2, "c");
      callableStatement.execute();
      assertEquals("sefc", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setBlob("t1", new ByteArrayInputStream("sef".getBytes()), 2);
      callableStatement.setString(2, "c");
      callableStatement.execute();
      assertEquals("sec", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setDate("t1", new Date(0));
      callableStatement.setString(2, "");
      callableStatement.execute();
      callableStatement.getDate("t2");

      reset(callableStatement);
      callableStatement.setDate("t1", new Date(0), Calendar.getInstance());
      callableStatement.setString(2, "");
      callableStatement.execute();
      callableStatement.getDate("t2", Calendar.getInstance());

      reset(callableStatement);
      callableStatement.setTime("t1", new Time(0));
      callableStatement.setString(2, "");
      callableStatement.execute();
      callableStatement.getTime("t2");

      reset(callableStatement);
      callableStatement.setTime("t1", new Time(0), Calendar.getInstance());
      callableStatement.setString(2, "");
      callableStatement.execute();
      callableStatement.getTime("t2", Calendar.getInstance());

      reset(callableStatement);
      callableStatement.setAsciiStream("t1", new ByteArrayInputStream("abc".getBytes()));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("abcb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setAsciiStream("t1", new ByteArrayInputStream("abc".getBytes()), 2);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("abcb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setAsciiStream("t1", new ByteArrayInputStream("abc".getBytes()), 2L);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("abb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setBinaryStream("t1", new ByteArrayInputStream("sef".getBytes()));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sefb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setBinaryStream("t1", new ByteArrayInputStream("sef".getBytes()), 2);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("seb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setBinaryStream("t1", new ByteArrayInputStream("sef".getBytes()), 2L);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("seb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setObject("t1", new ByteArrayInputStream("sef".getBytes()));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sefb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setObject(
          "t1", new ByteArrayInputStream("sef".getBytes()), Types.LONGVARBINARY);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sefb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setObject(
          "t1", new ByteArrayInputStream("sef".getBytes()), JDBCType.LONGVARBINARY);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sefb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setObject(
          "t1", new ByteArrayInputStream("sef".getBytes()), Types.LONGVARBINARY, 10);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sefb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setObject(
          "t1", new ByteArrayInputStream("sef".getBytes()), JDBCType.LONGVARBINARY, 10);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sefb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setCharacterStream("t1", new StringReader("sef"));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sefb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setCharacterStream("t1", new StringReader("sef"), 2);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("seb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setCharacterStream("t1", new StringReader("sef"), 2L);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("seb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNCharacterStream("t1", new StringReader("sef"));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sefb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNCharacterStream("t1", new StringReader("sef"), 2);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("seb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setDate("t1", new Date(0));
      callableStatement.setString(2, "b");
      callableStatement.execute();

      reset(callableStatement);
      callableStatement.setNull("t1", Types.BINARY);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertNull(callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNull("t1", Types.BINARY, "ttt");
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertNull(callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setClob("t1", new MariaDbClob("sef".getBytes(), 0, 16));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sefb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setClob("t1", new StringReader("sef"));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sefb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setClob("t1", new StringReader("sef"), 2);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("seb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNClob("t1", new MariaDbClob("sef".getBytes(), 0, 16));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sefb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNClob("t1", new StringReader("sef"));
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("sefb", callableStatement.getString(2));

      reset(callableStatement);
      callableStatement.setNClob("t1", new StringReader("sef"), 2);
      callableStatement.setString(2, "b");
      callableStatement.execute();
      assertEquals("seb", callableStatement.getString(2));

      assertThrowsContains(
          SQLFeatureNotSupportedException.class,
          () -> callableStatement.setRowId(1, null),
          "RowId parameter are not supported");
      assertThrowsContains(
          SQLFeatureNotSupportedException.class,
          () -> callableStatement.setRowId("t1", null),
          "RowId parameter are not supported");
      assertThrowsContains(
          SQLFeatureNotSupportedException.class,
          () -> callableStatement.setSQLXML(1, null),
          "SQLXML parameter are not supported");
      assertThrowsContains(
          SQLFeatureNotSupportedException.class,
          () -> callableStatement.setSQLXML("t1", null),
          "SQLXML parameter are not supported");
    }
  }

  private void reset(CallableStatement callableStatement) throws SQLException {
    callableStatement.registerOutParameter(2, JDBCType.VARCHAR);
    callableStatement.setString(1, "a");
    callableStatement.setString(2, "b");
    callableStatement.execute();
    assertEquals("ab", callableStatement.getString(2));
  }
}
