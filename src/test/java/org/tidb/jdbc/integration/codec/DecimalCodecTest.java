// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.integration.codec;

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
import org.tidb.jdbc.Statement;
import org.tidb.jdbc.integration.Common;

public class DecimalCodecTest extends CommonCodecTest {
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS DecimalCodec");
    stmt.execute("DROP TABLE IF EXISTS DecimalCodec2");
    stmt.execute("DROP TABLE IF EXISTS DecimalCodec3");
    stmt.execute("DROP TABLE IF EXISTS DecimalCodec4");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE DecimalCodec (t1 DECIMAL(10,0), t2 DECIMAL(30,6), t3 DECIMAL(10,3), t4 DECIMAL(10,0))");
    stmt.execute(
        "CREATE TABLE DecimalCodec2 (t1 DECIMAL(10,0), t2 DECIMAL(10,6), t3 DECIMAL(10,3), t4 DECIMAL(10,0))");
    stmt.execute(
        "INSERT INTO DecimalCodec VALUES (0, 105.21, -1.6, null), (0, 9223372036854775808, 0, null)");
    stmt.execute(
        "CREATE TABLE DecimalCodec3 (id int not null primary key auto_increment, t1 DECIMAL(10,0))");
    stmt.execute(
        "CREATE TABLE DecimalCodec4 (t1 DECIMAL(10,0) ZEROFILL, t2 DECIMAL(10,6) ZEROFILL, t3 DECIMAL(10,3) ZEROFILL, t4 DECIMAL(10,0) ZEROFILL)");
    stmt.execute("INSERT INTO DecimalCodec4 VALUES (0, 105.21, 1.6, null)");

    stmt.execute("FLUSH TABLES");
  }

  private ResultSet get() throws SQLException {
    return get(false);
  }

  private ResultSet get(boolean zerofill) throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from DecimalCodec"
                + (zerofill ? "4" : ""));
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private ResultSet getPrepare(Connection con) throws SQLException {
    return getPrepare(con, false);
  }

  private ResultSet getPrepare(Connection con, boolean zerofill) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    PreparedStatement preparedStatement =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from DecimalCodec"
                + (zerofill ? "4" : "")
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
    assertEquals(BigDecimal.ZERO, rs.getObject(1));
    assertFalse(rs.wasNull());
    assertEquals(new BigDecimal("105.210000"), rs.getObject(2));
    assertEquals(new BigDecimal("105.210000"), rs.getObject("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(new BigDecimal("-1.600"), rs.getObject(3));
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
    testObject(rs, String.class, "0");
    testObject(rs, Long.class, 0L);
    testObject(rs, Short.class, (short) 0);
    testObject(rs, BigDecimal.class, BigDecimal.valueOf(0));
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
    testErrObject(rs, LocalTime.class);
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
  public void getString() throws SQLException {
    getString(get());
    getStringZerofill(get(true));
  }

  @Test
  public void getStringPrepare() throws SQLException {
    getString(getPrepare(sharedConn));
    getString(getPrepare(sharedConnBinary));
    getStringZerofill(getPrepare(sharedConn, true));
    getStringZerofill(getPrepare(sharedConnBinary, true));
  }

  public void getString(ResultSet rs) throws SQLException {
    assertEquals("0", rs.getString(1));
    assertFalse(rs.wasNull());
    assertEquals("105.210000", rs.getString(2));
    assertEquals("105.210000", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("-1.600", rs.getString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getString(4));
    assertTrue(rs.wasNull());
  }

  public void getStringZerofill(ResultSet rs) throws SQLException {
    assertEquals("0", rs.getString(1));
    assertFalse(rs.wasNull());
    assertEquals("0105.210000", rs.getString(2));
    assertEquals("0105.210000", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("0000001.600", rs.getString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getString(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getNString() throws SQLException {
    getNString(get());
  }

  @Test
  public void getNStringPrepare() throws SQLException {
    getNString(getPrepare(sharedConn));
    getNString(getPrepare(sharedConnBinary));
  }

  public void getNString(ResultSet rs) throws SQLException {
    assertEquals("0", rs.getNString(1));
    assertFalse(rs.wasNull());
    assertEquals("105.210000", rs.getNString(2));
    assertEquals("105.210000", rs.getNString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("-1.600", rs.getNString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getNString(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBoolean() throws SQLException {
    getBoolean(get());
  }

  @Test
  public void getBooleanPrepare() throws SQLException {
    getBoolean(getPrepare(sharedConn));
    getBoolean(getPrepare(sharedConnBinary));
  }

  public void getBoolean(ResultSet rs) throws SQLException {
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
  public void getByte() throws SQLException {
    getByte(get());
  }

  @Test
  public void getBytePrepare() throws SQLException {
    getByte(getPrepare(sharedConn));
    getByte(getPrepare(sharedConnBinary));
  }

  public void getByte(ResultSet rs) throws SQLException {
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
  public void getShort() throws SQLException {
    getShort(get());
  }

  @Test
  public void getShortPrepare() throws SQLException {
    getShort(getPrepare(sharedConn));
    getShort(getPrepare(sharedConnBinary));
  }

  public void getShort(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getShort(1));
    assertFalse(rs.wasNull());
    assertEquals(105, rs.getShort(2));
    assertEquals(105, rs.getShort("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(-1, rs.getShort(3));
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getShort(4));
    assertTrue(rs.wasNull());
    assertTrue(rs.next());
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getShort(2),
        "value '9223372036854775808.000000' cannot be decoded as Short");
  }

  @Test
  public void getInt() throws SQLException {
    getInt(get());
  }

  @Test
  public void getIntPrepare() throws SQLException {
    getInt(getPrepare(sharedConn));
    getInt(getPrepare(sharedConnBinary));
  }

  public void getInt(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getInt(1));
    assertFalse(rs.wasNull());
    assertEquals(105, rs.getInt(2));
    assertEquals(105, rs.getInt("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(-1, rs.getInt(3));
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getInt(4));
    assertTrue(rs.wasNull());
    assertTrue(rs.next());
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getInt(2),
        "value '9223372036854775808.000000' cannot be decoded as Int");
  }

  @Test
  public void getLong() throws SQLException {
    getLong(get());
  }

  @Test
  public void getLongPrepare() throws SQLException {
    getLong(getPrepare(sharedConn));
    getLong(getPrepare(sharedConnBinary));
  }

  public void getLong(ResultSet rs) throws SQLException {
    assertEquals(0L, rs.getLong(1));
    assertFalse(rs.wasNull());
    assertEquals(105L, rs.getLong(2));
    assertEquals(105L, rs.getLong("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(-1L, rs.getLong(3));
    assertFalse(rs.wasNull());
    assertEquals(0L, rs.getLong(4));
    assertTrue(rs.wasNull());
    assertTrue(rs.next());
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getLong(2),
        "value '9223372036854775808.000000' cannot be decoded as Long");
  }

  @Test
  public void getFloat() throws SQLException {
    getFloat(get());
  }

  @Test
  public void getFloatPrepare() throws SQLException {
    getFloat(getPrepare(sharedConn));
    getFloat(getPrepare(sharedConnBinary));
  }

  public void getFloat(ResultSet rs) throws SQLException {
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
  public void getDouble() throws SQLException {
    getDouble(get());
  }

  @Test
  public void getDoublePrepare() throws SQLException {
    getDouble(getPrepare(sharedConn));
    getDouble(getPrepare(sharedConnBinary));
  }

  public void getDouble(ResultSet rs) throws SQLException {
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
  public void getBigDecimal() throws SQLException {
    getBigDecimal(get());
  }

  @Test
  public void getBigDecimalPrepare() throws SQLException {
    getBigDecimal(getPrepare(sharedConn));
    getBigDecimal(getPrepare(sharedConnBinary));
  }

  @SuppressWarnings("deprecation")
  public void getBigDecimal(ResultSet rs) throws SQLException {
    assertEquals(BigDecimal.ZERO, rs.getBigDecimal(1));
    assertFalse(rs.wasNull());
    assertEquals(new BigDecimal("105.210000"), rs.getBigDecimal(2));
    assertEquals(new BigDecimal("105.2"), rs.getBigDecimal(2, BigDecimal.ROUND_DOWN));
    assertEquals(new BigDecimal("105.210000"), rs.getBigDecimal("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(new BigDecimal("-1.600"), rs.getBigDecimal(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBigDecimal(4));
    assertNull(rs.getBigDecimal(4, BigDecimal.ROUND_CEILING));
    assertTrue(rs.wasNull());
    assertTrue(rs.next());
    assertEquals(new BigDecimal("9223372036854775808.000000"), rs.getBigDecimal(2));
  }

  @Test
  public void getBigDecimalScale() throws SQLException {
    getBigDecimalScale(get());
  }

  @Test
  public void getBigDecimalScalePrepare() throws SQLException {
    getBigDecimalScale(getPrepare(sharedConn));
    getBigDecimalScale(getPrepare(sharedConnBinary));
  }

  @SuppressWarnings("deprecation")
  public void getBigDecimalScale(ResultSet rs) throws SQLException {
    assertEquals(new BigDecimal("0.0"), rs.getBigDecimal(1, 1));
    assertFalse(rs.wasNull());
    assertEquals(new BigDecimal("0.0"), rs.getBigDecimal("t1alias", 1));
    assertFalse(rs.wasNull());
    assertEquals(new BigDecimal("105.2"), rs.getBigDecimal(2, 1));
    assertEquals(new BigDecimal("105.2"), rs.getBigDecimal("t2alias", 1));
    assertFalse(rs.wasNull());
    assertEquals(new BigDecimal("-1.6"), rs.getBigDecimal(3, 1));
    assertFalse(rs.wasNull());
    assertNull(rs.getBigDecimal(4, 1));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDate() throws SQLException {
    getDate(get());
  }

  @Test
  public void getDatePrepare() throws SQLException {
    getDate(getPrepare(sharedConn));
    getDate(getPrepare(sharedConnBinary));
  }

  public void getDate(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getDate(1), "Data type DECIMAL cannot be decoded as Date");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getDate("t1alias"),
        "Data type DECIMAL cannot be decoded as Date");
  }

  @Test
  public void getTime() throws SQLException {
    getTime(get());
  }

  @Test
  public void getTimePrepare() throws SQLException {
    getTime(getPrepare(sharedConn));
    getTime(getPrepare(sharedConnBinary));
  }

  public void getTime(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getTime(1), "Data type DECIMAL cannot be decoded as Time");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTime("t1alias"),
        "Data type DECIMAL cannot be decoded as Time");
  }

  @Test
  public void getTimestamp() throws SQLException {
    getTimestamp(get());
  }

  @Test
  public void getTimestampPrepare() throws SQLException {
    getTimestamp(getPrepare(sharedConn));
    getTimestamp(getPrepare(sharedConnBinary));
  }

  public void getTimestamp(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp(1),
        "Data type DECIMAL cannot be decoded as Timestamp");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp("t1alias"),
        "Data type DECIMAL cannot be decoded as Timestamp");
  }

  @Test
  public void getOffsetDateTime() throws SQLException {
    getOffsetDateTime(get());
  }

  @Test
  public void getOffsetDateTimePrepare() throws SQLException {
    getOffsetDateTime(getPrepare(sharedConn));
    getOffsetDateTime(getPrepare(sharedConnBinary));
  }

  public void getOffsetDateTime(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLException.class,
        () -> rs.getObject(1, OffsetDateTime.class),
        "Type class java.time.OffsetDateTime not supported type for DECIMAL type");
  }

  @Test
  public void getAsciiStream() throws SQLException {
    getAsciiStream(get());
  }

  @Test
  public void getAsciiStreamPrepare() throws SQLException {
    getAsciiStream(getPrepare(sharedConn));
    getAsciiStream(getPrepare(sharedConnBinary));
  }

  public void getAsciiStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getAsciiStream(1),
        "Data type DECIMAL cannot be decoded as Stream");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getAsciiStream("t1alias"),
        "Data type DECIMAL cannot be decoded as Stream");
  }

  @Test
  public void getUnicodeStream() throws SQLException {
    getUnicodeStream(get());
  }

  @Test
  public void getUnicodeStreamPrepare() throws SQLException {
    getUnicodeStream(getPrepare(sharedConn));
    getUnicodeStream(getPrepare(sharedConnBinary));
  }

  @SuppressWarnings("deprecation")
  public void getUnicodeStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getUnicodeStream(1),
        "Data type DECIMAL cannot be decoded as Stream");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getUnicodeStream("t1alias"),
        "Data type DECIMAL cannot be decoded as Stream");
  }

  @Test
  public void getBinaryStream() throws SQLException {
    getBinaryStream(get());
  }

  @Test
  public void getBinaryStreamPrepare() throws SQLException {
    getBinaryStream(getPrepare(sharedConn));
    getBinaryStream(getPrepare(sharedConnBinary));
  }

  public void getBinaryStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBinaryStream(1),
        "Data type DECIMAL cannot be decoded as Stream");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBinaryStream("t1alias"),
        "Data type DECIMAL cannot be decoded as Stream");
  }

  @Test
  public void getBytes() throws SQLException {
    getBytes(get());
  }

  @Test
  public void getBytesPrepare() throws SQLException {
    getBytes(getPrepare(sharedConn));
    getBytes(getPrepare(sharedConnBinary));
  }

  public void getBytes(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBytes(1),
        "Data type DECIMAL cannot be decoded as byte[]");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBytes("t1alias"),
        "Data type DECIMAL cannot be decoded as byte[]");
  }

  @Test
  public void getCharacterStream() throws SQLException {
    getCharacterStream(get());
  }

  @Test
  public void getCharacterStreamPrepare() throws SQLException {
    getCharacterStream(getPrepare(sharedConn));
    getCharacterStream(getPrepare(sharedConnBinary));
  }

  public void getCharacterStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type DECIMAL cannot be decoded as Reader");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream("t1alias"),
        "Data type DECIMAL cannot be decoded as Reader");
  }

  @Test
  public void getNCharacterStream() throws SQLException {
    getNCharacterStream(get());
  }

  @Test
  public void getNCharacterStreamPrepare() throws SQLException {
    getNCharacterStream(getPrepare(sharedConn));
    getNCharacterStream(getPrepare(sharedConnBinary));
  }

  public void getNCharacterStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream(1),
        "Data type DECIMAL cannot be decoded as Reader");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream("t2alias"),
        "Data type DECIMAL cannot be decoded as Reader");
  }

  @Test
  public void getRef() throws SQLException {
    getRef(get());
  }

  @Test
  public void getRefPrepare() throws SQLException {
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
  public void getBlob() throws SQLException {
    getBlob(get());
  }

  @Test
  public void getBlobPrepare() throws SQLException {
    getBlob(getPrepare(sharedConn));
    getBlob(getPrepare(sharedConnBinary));
  }

  public void getBlob(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type DECIMAL cannot be decoded as Reader");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream("t1alias"),
        "Data type DECIMAL cannot be decoded as Reader");
  }

  @Test
  public void getClob() throws SQLException {
    getClob(get());
  }

  @Test
  public void getClobPrepare() throws SQLException {
    getClob(getPrepare(sharedConn));
    getClob(getPrepare(sharedConnBinary));
  }

  public void getClob(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getClob(1), "Data type DECIMAL cannot be decoded as Clob");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getClob("t1alias"),
        "Data type DECIMAL cannot be decoded as Clob");
  }

  @Test
  public void getNClob() throws SQLException {
    getNClob(get());
  }

  @Test
  public void getNClobPrepare() throws SQLException {
    getNClob(getPrepare(sharedConn));
    getNClob(getPrepare(sharedConnBinary));
  }

  public void getNClob(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNClob(1),
        "Data type DECIMAL cannot be decoded as Clob");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNClob("t1alias"),
        "Data type DECIMAL cannot be decoded as Clob");
  }

  @Test
  public void getURL() throws SQLException {
    getURL(get());
  }

  @Test
  public void getURLPrepare() throws SQLException {
    getURL(getPrepare(sharedConn));
    getURL(getPrepare(sharedConnBinary));
  }

  public void getURL(ResultSet rs) {
    Common.assertThrowsContains(
        SQLSyntaxErrorException.class, () -> rs.getURL(1), "Could not parse '0' as URL");
    Common.assertThrowsContains(
        SQLSyntaxErrorException.class, () -> rs.getURL("t1alias"), "Could not parse '0' as URL");
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = get();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("DECIMAL", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.math.BigDecimal", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.DECIMAL, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(10, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(11, meta.getColumnDisplaySize(1));

    assertEquals(30, meta.getPrecision(2));
    assertEquals(6, meta.getScale(2));
    assertEquals("", meta.getSchemaName(2));
    assertEquals(32, meta.getColumnDisplaySize(2));
  }

  @Test
  public void setParameter() throws SQLException {
    try (PreparedStatement prep =
        sharedConn.prepareStatement("INSERT INTO DecimalCodec2 VALUE (?, ?, ?, ?)")) {
      prep.setBigDecimal(1, new BigDecimal("789.123"));
      prep.setBigDecimal(2, new BigDecimal("1789.123456"));
      prep.setNull(3, Types.DECIMAL);
      prep.setObject(4, new BigDecimal("-211789.987987"));
      prep.execute();
      Statement stmt = sharedConn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT * FROM DecimalCodec2");
      rs.next();
      assertEquals(new BigDecimal("789"), rs.getBigDecimal(1));
      assertEquals(new BigDecimal("1789.123456"), rs.getBigDecimal(2));
      assertNull(rs.getBigDecimal(3));
      assertEquals(new BigDecimal("-211790"), rs.getBigDecimal(4));
      Common.assertThrowsContains(
          SQLException.class,
          () -> prep.setObject(4, this),
          "Type org.tidb.jdbc.integration.codec.DecimalCodecTest not supported type");
    }
  }

  @Test
  public void sendParam() throws SQLException {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE DecimalCodec3");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO DecimalCodec3(t1) VALUES (?)")) {
      prep.setBigDecimal(1, BigDecimal.valueOf(1));
      prep.execute();
      prep.setBigDecimal(1, null);
      prep.execute();
      prep.setObject(1, BigDecimal.valueOf(2));
      prep.execute();
      prep.setObject(1, null);
      prep.execute();
      prep.setObject(1, BigDecimal.valueOf(3), Types.DECIMAL);
      prep.execute();
      prep.setObject(1, "4", Types.DECIMAL);
      prep.execute();
      prep.setObject(1, "5", Types.NUMERIC);
      prep.execute();
      prep.setObject(1, 7D, JDBCType.NUMERIC);
      prep.execute();
      prep.setObject(1, "6", Types.BIGINT);
      prep.execute();
      prep.setObject(1, null, Types.DECIMAL);
      prep.execute();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM DecimalCodec3");

    assertTrue(rs.next());
    assertEquals("1", rs.getBigDecimal(2).toString());
    rs.updateBigDecimal(2, null);
    rs.updateRow();
    assertNull(rs.getBigDecimal(2));

    assertTrue(rs.next());
    assertNull(rs.getBigDecimal(2));
    rs.updateBigDecimal("t1", BigDecimal.ONE);
    rs.updateRow();
    assertEquals("1", rs.getBigDecimal(2).toString());

    assertTrue(rs.next());
    assertEquals("2", rs.getBigDecimal(2).toString());
    rs.updateObject(2, null);
    rs.updateRow();
    assertNull(rs.getBigDecimal(2));

    assertTrue(rs.next());
    assertNull(rs.getString(2));
    rs.updateObject(2, BigDecimal.valueOf(20));
    rs.updateRow();
    assertEquals(BigDecimal.valueOf(20), rs.getBigDecimal(2));

    assertTrue(rs.next());
    assertEquals("3", rs.getBigDecimal(2).toString());
    rs.updateObject("t1", null, Types.DECIMAL);
    rs.updateRow();
    assertNull(rs.getBigDecimal(2));

    assertTrue(rs.next());
    assertEquals("4", rs.getBigDecimal(2).toString());

    assertTrue(rs.next());
    assertEquals("5", rs.getBigDecimal(2).toString());

    assertTrue(rs.next());
    assertEquals("7", rs.getBigDecimal(2).toString());

    assertTrue(rs.next());
    assertEquals("6", rs.getBigDecimal(2).toString());

    assertTrue(rs.next());
    assertNull(rs.getString(2));
    rs.updateObject(2, BigDecimal.valueOf(30), Types.DECIMAL);
    rs.updateRow();
    assertEquals(BigDecimal.valueOf(30), rs.getBigDecimal(2));

    rs = stmt.executeQuery("SELECT * FROM DecimalCodec3");

    assertTrue(rs.next());
    assertNull(rs.getBigDecimal(2));

    assertTrue(rs.next());
    assertEquals("1", rs.getBigDecimal(2).toString());

    assertTrue(rs.next());
    assertNull(rs.getBigDecimal(2));

    assertTrue(rs.next());
    assertEquals(BigDecimal.valueOf(20), rs.getBigDecimal(2));

    assertTrue(rs.next());
    assertNull(rs.getBigDecimal(2));

    assertTrue(rs.next());
    assertEquals("4", rs.getBigDecimal(2).toString());
    assertTrue(rs.next());
    assertEquals("5", rs.getBigDecimal(2).toString());
    assertTrue(rs.next());
    assertEquals("7", rs.getBigDecimal(2).toString());
    assertTrue(rs.next());
    assertEquals("6", rs.getBigDecimal(2).toString());

    assertTrue(rs.next());
    assertEquals(BigDecimal.valueOf(30), rs.getBigDecimal(2));
  }
}
