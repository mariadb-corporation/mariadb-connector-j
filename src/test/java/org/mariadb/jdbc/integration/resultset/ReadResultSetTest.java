// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.integration.resultset;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.integration.Common;

public class ReadResultSetTest extends Common {

  private final String NOT_SUPPORTED = "Not supported when using CONCUR_READ_ONLY concurrency";
  private final Class<? extends java.lang.Exception> ns = SQLFeatureNotSupportedException.class;

  @AfterAll
  public static void after2() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE ResultSetTest");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS ResultSetTest");
    stmt.execute("CREATE TABLE ResultSetTest (t1 int not null primary key auto_increment, t2 int)");
    stmt.execute("INSERT INTO ResultSetTest(t2) values (1),(2),(3),(4),(5),(6),(7),(8)");
  }

  @Test
  public void nonUpdatableFields() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    Assertions.assertNull(rs.getWarnings());
    rs.next();
    Common.assertThrowsContains(ns, () -> rs.updateArray(1, null), "Array are not supported");
    Common.assertThrowsContains(ns, () -> rs.updateNull(1), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBoolean(1, true), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateByte(1, (byte) 0x00), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateShort(1, (short) 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateInt(1, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateLong(1, 0L), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateFloat(1, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateDouble(1, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBigDecimal(1, BigDecimal.ZERO), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateString(1, ""), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateNString(1, ""), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBytes(1, new byte[0]), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateDate(1, null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateTime(1, null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateTimestamp(1, null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateAsciiStream(1, null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateAsciiStream(1, null, 0L), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBinaryStream(1, null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateCharacterStream(1, null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBinaryStream(1, null, 0L), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateCharacterStream(1, null, 0L), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateObject(1, null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateObject(1, null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBlob(1, (Blob) null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateClob(1, (Clob) null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBlob(1, (InputStream) null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateClob(1, (Reader) null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateNClob(1, (NClob) null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateNClob(1, (Reader) null), NOT_SUPPORTED);

    Common.assertThrowsContains(ns, () -> rs.updateArray("t1", null), "Array are not supported");

    Common.assertThrowsContains(ns, () -> rs.updateNull("t1"), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBoolean("t1", true), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateByte("t1", (byte) 0x00), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateShort("t1", (short) 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateInt("t1", 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateLong("t1", 0L), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateFloat("t1", 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateDouble("t1", 0), NOT_SUPPORTED);
    Common.assertThrowsContains(
        ns, () -> rs.updateBigDecimal("t1", BigDecimal.ZERO), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateString("t1", ""), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateNString("t1", ""), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBytes("t1", new byte[0]), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateDate("t1", null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateTime("t1", null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateTimestamp("t1", null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateAsciiStream("t1", null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBinaryStream("t1", null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateCharacterStream("t1", null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateAsciiStream("t1", null, 0L), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBinaryStream("t1", null, 0L), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateCharacterStream("t1", null, 0L), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateObject("t1", null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateObject("t1", null), NOT_SUPPORTED);

    Common.assertThrowsContains(ns, () -> rs.updateBlob("t1", (Blob) null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBlob("t1", (InputStream) null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateClob("t1", (Clob) null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateClob("t1", (Reader) null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateNClob("t1", (NClob) null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateNClob("t1", (Reader) null), NOT_SUPPORTED);

    Common.assertThrowsContains(ns, rs::insertRow, NOT_SUPPORTED);
    Common.assertThrowsContains(ns, rs::updateRow, NOT_SUPPORTED);
    Common.assertThrowsContains(ns, rs::deleteRow, NOT_SUPPORTED);
    Common.assertThrowsContains(ns, rs::refreshRow, NOT_SUPPORTED);
    Common.assertThrowsContains(ns, rs::cancelRowUpdates, NOT_SUPPORTED);
    Common.assertThrowsContains(ns, rs::moveToInsertRow, NOT_SUPPORTED);
    Common.assertThrowsContains(ns, rs::moveToCurrentRow, NOT_SUPPORTED);
    Common.assertThrowsContains(ns, rs::rowUpdated, NOT_SUPPORTED);
    Common.assertThrowsContains(ns, rs::rowInserted, NOT_SUPPORTED);
    Common.assertThrowsContains(ns, rs::rowDeleted, NOT_SUPPORTED);

    Common.assertThrowsContains(
        ns, () -> rs.updateRef(1, null), "Method ResultSet.updateRef not supported");
    Common.assertThrowsContains(
        ns, () -> rs.updateRef("t1", null), "Method ResultSet.updateRef not supported");
    Common.assertThrowsContains(ns, () -> rs.updateArray(1, null), "Array are not supported");
    Common.assertThrowsContains(ns, () -> rs.updateArray("t1", null), "Array are not supported");
    Common.assertThrowsContains(ns, () -> rs.updateRowId(1, null), "RowId are not supported");
    Common.assertThrowsContains(ns, () -> rs.updateRowId("t1", null), "RowId are not supported");
    Common.assertThrowsContains(ns, () -> rs.updateSQLXML(1, null), "SQLXML not supported");
    Common.assertThrowsContains(ns, () -> rs.updateSQLXML("t1", null), "SQLXML not supported");

    Common.assertThrowsContains(ns, () -> rs.updateNCharacterStream(1, null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateNCharacterStream("t1", null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateAsciiStream(1, null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateAsciiStream("t1", null, 0), NOT_SUPPORTED);

    Common.assertThrowsContains(ns, () -> rs.updateBinaryStream(1, null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBinaryStream("t1", null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBlob(1, null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBlob("t1", null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateClob(1, null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateClob("t1", null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateNClob(1, null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateNClob("t1", null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateCharacterStream(1, null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateCharacterStream("t1", null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateNCharacterStream(1, null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateNCharacterStream("t1", null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateAsciiStream(1, null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateAsciiStream("t1", null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBinaryStream(1, null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateBinaryStream("t1", null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateObject(1, null, null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateObject("t1", null, null, 0), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateObject(1, null, null), NOT_SUPPORTED);
    Common.assertThrowsContains(ns, () -> rs.updateObject("t1", null, null), NOT_SUPPORTED);
  }

  @Test
  public void notSupported() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    Common.assertThrowsContains(ns, () -> rs.getRowId(1), "RowId are not supported");
    Common.assertThrowsContains(ns, () -> rs.getRowId("t1"), "RowId are not supported");
    Map<String, Class<?>> map = new HashMap<>();
    map.put("ddd", Integer.class);
    Common.assertThrowsContains(
        ns,
        () -> rs.getObject(1, map),
        "Method ResultSet.getObject(int columnIndex, Map<String, Class<?>> map) not supported");
    Common.assertThrowsContains(
        ns,
        () -> rs.getObject("t1", map),
        "Method ResultSet.getObject(String columnLabel, Map<String, Class<?>> map) not supported");
    Common.assertThrowsContains(ns, rs::getCursorName, "Cursors are not supported");
  }

  @Test
  public void staticMethod() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    Assertions.assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, rs.getHoldability());
    rs.unwrap(ResultSet.class);

    Common.assertThrowsContains(
        SQLException.class,
        () -> rs.unwrap(String.class),
        "The receiver is not a wrapper for java.lang.String");

    assertEquals(ResultSet.FETCH_UNKNOWN, rs.getFetchDirection());
    rs.setFetchDirection(ResultSet.FETCH_UNKNOWN);
    Common.assertThrowsContains(
        SQLException.class,
        () -> rs.setFetchDirection(ResultSet.FETCH_REVERSE),
        "Invalid operation");
    assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
  }

  @Test
  public void type() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
    assertEquals(stmt, rs.getStatement());

    stmt = sharedConn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
    rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
    assertEquals(stmt, rs.getStatement());
  }

  @Test
  public void wrongIndex() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    rs.next();
    Common.assertThrowsContains(
        SQLException.class,
        () -> rs.getObject(-1),
        "Wrong index position. Is -1 but must be in 1-2 range");
    Common.assertThrowsContains(
        SQLException.class,
        () -> rs.getObject(10),
        "Wrong index position. Is 10 but must be in 1-2 range");
    Common.assertThrowsContains(
        SQLException.class, () -> rs.findColumn(null), "null is not a valid label value");
    Common.assertThrowsContains(
        SQLException.class,
        () -> rs.findColumn("yy"),
        "Unknown label 'yy'. Possible value [resultsettest.t1, t1, resultsettest.t2, t2]");
  }

  @Test
  public void isBeforeFirstFetchTest() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(1);
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    assertTrue(rs.isBeforeFirst());
    while (rs.next()) {
      assertFalse(rs.isBeforeFirst());
    }
    assertFalse(rs.isBeforeFirst());
    rs.close();
    Common.assertThrowsContains(
        SQLException.class, rs::isBeforeFirst, "Operation not permit on a closed resultSet");
  }
}
