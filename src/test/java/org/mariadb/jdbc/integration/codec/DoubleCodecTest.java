// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.integration.Common;

public class DoubleCodecTest extends CommonCodecTest {
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS DoubleCodec");
    stmt.execute("DROP TABLE IF EXISTS DoubleCodec2");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE DoubleCodec (t1 DOUBLE, t2 DOUBLE, t3 DOUBLE, t4 DOUBLE)");
    stmt.execute("INSERT INTO DoubleCodec VALUES (0, 105.21, -1.6, null)");
    stmt.execute(
        "CREATE TABLE DoubleCodec2 (id int not null primary key auto_increment, t1 DOUBLE)");
    stmt.execute("FLUSH TABLES");
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from DoubleCodec");
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private ResultSet getPrepare(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    PreparedStatement preparedStatement =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from DoubleCodec"
                + " WHERE 1 > ?");
    preparedStatement.closeOnCompletion();
    preparedStatement.setInt(1, 0);
    ResultSet rs = preparedStatement.executeQuery();
    assertTrue(rs.next());
    con.commit();
    return rs;
  }

  @Test
  public void getObject() throws SQLException {
    getObject(get());
  }

  @Test
  public void getObjectPrepare() throws SQLException {
    getObject(getPrepare(sharedConn));
    getObject(getPrepare(sharedConnBinary));
  }

  public void getObject(ResultSet rs) throws SQLException {
    assertEquals(0d, (double) rs.getObject(1), 0.00001d);
    assertFalse(rs.wasNull());
    assertEquals(105.21d, (double) rs.getObject(2), 0.00001d);
    assertEquals(105.21d, (double) rs.getObject("t2alias"), 0.00001d);
    assertFalse(rs.wasNull());
    assertEquals(-1.6d, (double) rs.getObject(3), 0.00001d);
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getObjectType() throws Exception {
    getObjectType(get());
  }

  @Test
  public void getObjectTypePrepare() throws Exception {
    getObjectType(getPrepare(sharedConn));
    getObjectType(getPrepare(sharedConnBinary));
  }

  public void getObjectType(ResultSet rs) throws Exception {
    testObject(rs, Integer.class, 0);
    //    testObject(rs, String.class, "0");
    testObject(rs, Long.class, 0L);
    testObject(rs, Short.class, (short) 0);
    //    testObject(rs, BigDecimal.class, BigDecimal.valueOf(0));
    testObject(rs, BigInteger.class, BigInteger.valueOf(0));
    testObject(rs, Double.class, (double) 0);
    testObject(rs, Float.class, (float) 0);
    testObject(rs, Byte.class, (byte) 0);
    testErrObject(rs, byte[].class);
    testErrObject(rs, Date.class);
    testErrObject(rs, Time.class);
    testErrObject(rs, Timestamp.class);
    testErrObject(rs, java.util.Date.class);
    testErrObject(rs, LocalDate.class);
    testErrObject(rs, ZonedDateTime.class);
    testErrObject(rs, OffsetDateTime.class);
    testErrObject(rs, LocalDateTime.class);
    testErrObject(rs, OffsetTime.class);
    testObject(rs, Boolean.class, Boolean.FALSE);
    testErrObject(rs, Clob.class);
    testErrObject(rs, NClob.class);
    testErrObject(rs, InputStream.class);
    testErrObject(rs, Reader.class);
  }

  @Test
  public void getString() throws Exception {
    getString(get());
  }

  @Test
  public void getStringPrepare() throws Exception {
    getString(getPrepare(sharedConn));
    getString(getPrepare(sharedConnBinary));
  }

  public void getString(ResultSet rs) throws Exception {
    assertTrue("0".equals(rs.getString(1)) || "0.0".equals(rs.getString(1)));
    assertFalse(rs.wasNull());
    assertEquals("105.21", rs.getString(2));
    assertEquals("105.21", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("-1.6", rs.getString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getString(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getNString() throws Exception {
    getNString(get());
  }

  @Test
  public void getNStringPrepare() throws Exception {
    getNString(getPrepare(sharedConn));
    getNString(getPrepare(sharedConnBinary));
  }

  public void getNString(ResultSet rs) throws Exception {
    assertTrue("0".equals(rs.getString(1)) || "0.0".equals(rs.getString(1)));
    assertFalse(rs.wasNull());
    assertEquals("105.21", rs.getNString(2));
    assertEquals("105.21", rs.getNString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("-1.6", rs.getNString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getNString(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBoolean() throws Exception {
    getBoolean(get());
  }

  @Test
  public void getBooleanPrepare() throws Exception {
    getBoolean(getPrepare(sharedConn));
    getBoolean(getPrepare(sharedConnBinary));
  }

  public void getBoolean(ResultSet rs) throws Exception {
    assertFalse(rs.getBoolean(1));
    assertFalse(rs.wasNull());
    assertTrue(rs.getBoolean(2));
    assertTrue(rs.getBoolean("t2alias"));
    assertFalse(rs.wasNull());
    assertTrue(rs.getBoolean(3));
    assertFalse(rs.wasNull());
    assertFalse(rs.getBoolean(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getByte() throws Exception {
    getByte(get());
  }

  @Test
  public void getBytePrepare() throws Exception {
    getByte(getPrepare(sharedConn));
    getByte(getPrepare(sharedConnBinary));
  }

  public void getByte(ResultSet rs) throws Exception {
    assertEquals((byte) 0, rs.getByte(1));
    assertFalse(rs.wasNull());
    assertEquals((byte) 105, rs.getByte(2));
    assertEquals((byte) 105, rs.getByte("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals((byte) -1, rs.getByte(3));
    assertFalse(rs.wasNull());
    assertEquals((byte) 0, rs.getByte(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getShort() throws Exception {
    getShort(get());
  }

  @Test
  public void getShortPrepare() throws Exception {
    getShort(getPrepare(sharedConn));
    getShort(getPrepare(sharedConnBinary));
  }

  public void getShort(ResultSet rs) throws Exception {
    assertEquals(0, rs.getShort(1));
    assertFalse(rs.wasNull());
    assertEquals(105, rs.getShort(2));
    assertEquals(105, rs.getShort("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(-1, rs.getShort(3));
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getShort(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getInt() throws Exception {
    getInt(get());
  }

  @Test
  public void getIntPrepare() throws Exception {
    getInt(getPrepare(sharedConn));
    getInt(getPrepare(sharedConnBinary));
  }

  public void getInt(ResultSet rs) throws Exception {
    assertEquals(0, rs.getInt(1));
    assertFalse(rs.wasNull());
    assertEquals(105, rs.getInt(2));
    assertEquals(105, rs.getInt("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(-1, rs.getInt(3));
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getInt(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getLong() throws Exception {
    getLong(get());
  }

  @Test
  public void getLongPrepare() throws Exception {
    getLong(getPrepare(sharedConn));
    getLong(getPrepare(sharedConnBinary));
  }

  public void getLong(ResultSet rs) throws Exception {
    assertEquals(0L, rs.getLong(1));
    assertFalse(rs.wasNull());
    assertEquals(105L, rs.getLong(2));
    assertEquals(105L, rs.getLong("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(-1L, rs.getLong(3));
    assertFalse(rs.wasNull());
    assertEquals(0L, rs.getLong(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getFloat() throws Exception {
    getFloat(get());
  }

  @Test
  public void getFloatPrepare() throws Exception {
    getFloat(getPrepare(sharedConn));
    getFloat(getPrepare(sharedConnBinary));
  }

  public void getFloat(ResultSet rs) throws Exception {
    assertEquals(0F, rs.getFloat(1));
    assertFalse(rs.wasNull());
    assertEquals(105.21F, rs.getFloat(2));
    assertEquals(105.21F, rs.getFloat("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(-1.6F, rs.getFloat(3));
    assertFalse(rs.wasNull());
    assertEquals(0F, rs.getFloat(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDouble() throws Exception {
    getDouble(get());
  }

  @Test
  public void getDoublePrepare() throws Exception {
    getDouble(getPrepare(sharedConn));
    getDouble(getPrepare(sharedConnBinary));
  }

  public void getDouble(ResultSet rs) throws Exception {
    assertEquals(0D, rs.getDouble(1));
    assertFalse(rs.wasNull());
    assertEquals(105.21D, rs.getDouble(2));
    assertEquals(105.21D, rs.getDouble("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(-1.6D, rs.getDouble(3));
    assertFalse(rs.wasNull());
    assertEquals(0D, rs.getDouble(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBigDecimal() throws Exception {
    getBigDecimal(get());
  }

  @Test
  public void getBigDecimalPrepare() throws Exception {
    getBigDecimal(getPrepare(sharedConn));
    getBigDecimal(getPrepare(sharedConnBinary));
  }

  public void getBigDecimal(ResultSet rs) throws Exception {
    assertTrue(
        BigDecimal.ZERO.equals(rs.getBigDecimal(1))
            || new BigDecimal("0.0").equals(rs.getBigDecimal(1)));
    assertFalse(rs.wasNull());
    assertEquals(new BigDecimal("105.21"), rs.getBigDecimal(2));
    assertEquals(new BigDecimal("105.21"), rs.getBigDecimal("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(new BigDecimal("-1.6"), rs.getBigDecimal(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBigDecimal(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDate() throws Exception {
    getDate(get());
  }

  @Test
  public void getDatePrepare() throws Exception {
    getDate(getPrepare(sharedConn));
    getDate(getPrepare(sharedConnBinary));
  }

  public void getDate(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getDate(1), "Data type DOUBLE cannot be decoded as Date");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getDate("t1alias"),
        "Data type DOUBLE cannot be decoded as Date");
  }

  @Test
  public void getTime() throws Exception {
    getTime(get());
  }

  @Test
  public void getTimePrepare() throws Exception {
    getTime(getPrepare(sharedConn));
    getTime(getPrepare(sharedConnBinary));
  }

  public void getTime(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getTime(1), "Data type DOUBLE cannot be decoded as Time");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTime("t1alias"),
        "Data type DOUBLE cannot be decoded as Time");
  }

  @Test
  public void getTimestamp() throws Exception {
    getTimestamp(get());
  }

  @Test
  public void getTimestampPrepare() throws Exception {
    getTimestamp(getPrepare(sharedConn));
    getTimestamp(getPrepare(sharedConnBinary));
  }

  public void getTimestamp(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp(1),
        "Data type DOUBLE cannot be decoded as Timestamp");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp("t1alias"),
        "Data type DOUBLE cannot be decoded as Timestamp");
  }

  @Test
  public void getAsciiStream() throws Exception {
    getAsciiStream(get());
  }

  @Test
  public void getAsciiStreamPrepare() throws Exception {
    getAsciiStream(getPrepare(sharedConn));
    getAsciiStream(getPrepare(sharedConnBinary));
  }

  public void getAsciiStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getAsciiStream(1),
        "Data type DOUBLE cannot be decoded as Stream");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getAsciiStream("t1alias"),
        "Data type DOUBLE cannot be decoded as Stream");
  }

  @Test
  public void getUnicodeStream() throws Exception {
    getUnicodeStream(get());
  }

  @Test
  public void getUnicodeStreamPrepare() throws Exception {
    getUnicodeStream(getPrepare(sharedConn));
    getUnicodeStream(getPrepare(sharedConnBinary));
  }

  @SuppressWarnings("deprecation")
  public void getUnicodeStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getUnicodeStream(1),
        "Data type DOUBLE cannot be decoded as Stream");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getUnicodeStream("t1alias"),
        "Data type DOUBLE cannot be decoded as Stream");
  }

  @Test
  public void getBinaryStream() throws Exception {
    getBinaryStream(get());
  }

  @Test
  public void getBinaryStreamPrepare() throws Exception {
    getBinaryStream(getPrepare(sharedConn));
    getBinaryStream(getPrepare(sharedConnBinary));
  }

  public void getBinaryStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBinaryStream(1),
        "Data type DOUBLE cannot be decoded as Stream");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBinaryStream("t1alias"),
        "Data type DOUBLE cannot be decoded as Stream");
  }

  @Test
  public void getBytes() throws Exception {
    getBytes(get());
  }

  @Test
  public void getBytesPrepare() throws Exception {
    getBytes(getPrepare(sharedConn));
    getBytes(getPrepare(sharedConnBinary));
  }

  public void getBytes(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBytes(1),
        "Data type DOUBLE cannot be decoded as byte[]");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBytes("t1alias"),
        "Data type DOUBLE cannot be decoded as byte[]");
  }

  @Test
  public void getCharacterStream() throws Exception {
    getCharacterStream(get());
  }

  @Test
  public void getCharacterStreamPrepare() throws Exception {
    getCharacterStream(getPrepare(sharedConn));
    getCharacterStream(getPrepare(sharedConnBinary));
  }

  public void getCharacterStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type DOUBLE cannot be decoded as Reader");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream("t1alias"),
        "Data type DOUBLE cannot be decoded as Reader");
  }

  @Test
  public void getNCharacterStream() throws Exception {
    getNCharacterStream(get());
  }

  @Test
  public void getNCharacterStreamPrepare() throws Exception {
    getNCharacterStream(getPrepare(sharedConn));
    getNCharacterStream(getPrepare(sharedConnBinary));
  }

  public void getNCharacterStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream(1),
        "Data type DOUBLE cannot be decoded as Reader");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream("t2alias"),
        "Data type DOUBLE cannot be decoded as Reader");
  }

  @Test
  public void getRef() throws Exception {
    getRef(get());
  }

  @Test
  public void getRefPrepare() throws Exception {
    getRef(getPrepare(sharedConn));
    getRef(getPrepare(sharedConnBinary));
  }

  public void getRef(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class, () -> rs.getRef(1), "Method ResultSet.getRef not supported");
    Common.assertThrowsContains(
        SQLException.class, () -> rs.getRef("t2alias"), "Method ResultSet.getRef not supported");
  }

  @Test
  public void getBlob() throws Exception {
    getBlob(get());
  }

  @Test
  public void getBlobPrepare() throws Exception {
    getBlob(getPrepare(sharedConn));
    getBlob(getPrepare(sharedConnBinary));
  }

  public void getBlob(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type DOUBLE cannot be decoded as Reader");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream("t1alias"),
        "Data type DOUBLE cannot be decoded as Reader");
  }

  @Test
  public void getClob() throws Exception {
    getClob(get());
  }

  @Test
  public void getClobPrepare() throws Exception {
    getClob(getPrepare(sharedConn));
    getClob(getPrepare(sharedConnBinary));
  }

  public void getClob(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getClob(1), "Data type DOUBLE cannot be decoded as Clob");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getClob("t1alias"),
        "Data type DOUBLE cannot be decoded as Clob");
  }

  @Test
  public void getNClob() throws Exception {
    getNClob(get());
  }

  @Test
  public void getNClobPrepare() throws Exception {
    getNClob(getPrepare(sharedConn));
    getNClob(getPrepare(sharedConnBinary));
  }

  public void getNClob(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getNClob(1), "Data type DOUBLE cannot be decoded as Clob");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNClob("t1alias"),
        "Data type DOUBLE cannot be decoded as Clob");
  }

  @Test
  public void getURL() throws Exception {
    getURL(get());
  }

  @Test
  public void getURLPrepare() throws Exception {
    getURL(getPrepare(sharedConn));
    getURL(getPrepare(sharedConnBinary));
  }

  public void getURL(ResultSet rs) {
    Common.assertThrowsContains(
        SQLSyntaxErrorException.class, () -> rs.getURL(1), "Could not parse");
    Common.assertThrowsContains(
        SQLSyntaxErrorException.class, () -> rs.getURL("t1alias"), "Could not parse");
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = get();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("DOUBLE", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.lang.Double", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.DOUBLE, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(22, meta.getPrecision(1));
    assertEquals(31, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(22, meta.getColumnDisplaySize(1));
  }

  @Test
  public void sendParam() throws SQLException {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE DoubleCodec2");
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO DoubleCodec2(t1) VALUES (?)")) {
      prep.setDouble(1, 1D);
      prep.execute();
      prep.setObject(1, 2D);
      prep.execute();
      prep.setObject(1, null);
      prep.execute();
      prep.setObject(1, 3D, Types.DECIMAL);
      prep.execute();
      prep.setObject(1, null, Types.DECIMAL);
      prep.execute();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM DoubleCodec2");
    assertTrue(rs.next());
    assertEquals(1D, rs.getDouble(2));
    rs.updateDouble("t1", 10D);
    rs.updateRow();
    assertEquals(10D, rs.getDouble(2));

    assertTrue(rs.next());
    assertEquals(2D, rs.getDouble(2));
    rs.updateObject(2, null);
    rs.updateRow();
    assertNull(rs.getString(2));

    assertTrue(rs.next());
    assertNull(rs.getString(2));
    rs.updateObject("t1", 20D);
    rs.updateRow();
    assertEquals(20D, rs.getDouble(2));

    assertTrue(rs.next());
    assertEquals(3D, rs.getDouble(2));
    rs.updateObject(2, null, Types.DECIMAL);
    rs.updateRow();
    assertNull(rs.getString(2));

    assertTrue(rs.next());
    assertNull(rs.getString(2));
    rs.updateObject("t1", 30D, Types.DECIMAL);
    rs.updateRow();
    assertEquals(30D, rs.getDouble(2));

    rs = stmt.executeQuery("SELECT * FROM DoubleCodec2");
    assertTrue(rs.next());
    assertEquals(10D, rs.getDouble(2));

    assertTrue(rs.next());
    assertNull(rs.getString(2));

    assertTrue(rs.next());
    assertEquals(20D, rs.getDouble(2));

    assertTrue(rs.next());
    assertNull(rs.getString(2));

    assertTrue(rs.next());
    assertEquals(30D, rs.getDouble(2));
  }
}
