// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.*;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.integration.Common;

public class YearCodecTest extends CommonCodecTest {
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS YearCodec");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    if (isMariaDBServer()) {
      stmt.execute("CREATE TABLE YearCodec (t1 YEAR(2), t2 YEAR(4), t3 YEAR(4), t4 YEAR(4))");
      stmt.execute(
          "INSERT INTO YearCodec VALUES ('2010', '1901', '2155', null), (80, '1901', '2155',"
              + " null)");
    } else {
      stmt.execute("CREATE TABLE YearCodec (t1 YEAR(4), t2 YEAR(4), t3 YEAR(4), t4 YEAR(4))");
      stmt.execute(
          "INSERT INTO YearCodec VALUES ('2010', '1901', '2155', null), (1980, '1901', '2155',"
              + " null)");
    }
    stmt.execute("FLUSH TABLES");
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from YearCodec");
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private ResultSet getPrepare(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    PreparedStatement preparedStatement =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from YearCodec"
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
    assertFalse(rs.wasNull());
    assertEquals(Date.valueOf("2010-01-01").getTime(), ((Date) rs.getObject(1)).getTime());
    assertFalse(rs.wasNull());
    assertFalse(rs.wasNull());
    assertEquals(Date.valueOf("1901-01-01").getTime(), ((Date) rs.getObject(2)).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Date.valueOf("2155-01-01").getTime(), ((Date) rs.getObject(3)).getTime());
    assertFalse(rs.wasNull());
    assertNull(rs.getDate(4));
    assertTrue(rs.wasNull());
    rs.next();
    assertEquals(Date.valueOf("1980-01-01").getTime(), ((Date) rs.getObject(1)).getTime());
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
    testObject(rs, Integer.class, isMariaDBServer() ? 10 : 2010);
    testObject(rs, String.class, isMariaDBServer() ? "10" : "2010");
    testObject(rs, Long.class, isMariaDBServer() ? 10L : 2010L);
    testObject(rs, Short.class, (short) (isMariaDBServer() ? 10 : 2010));
    testObject(rs, BigDecimal.class, isMariaDBServer() ? BigDecimal.TEN : BigDecimal.valueOf(2010));
    testObject(rs, BigInteger.class, isMariaDBServer() ? BigInteger.TEN : BigInteger.valueOf(2010));
    testObject(rs, Double.class, isMariaDBServer() ? 10d : 2010d);
    testObject(rs, Float.class, isMariaDBServer() ? 10f : 2010f);
    if (isMariaDBServer()) {
      testObject(rs, Byte.class, (byte) 0x0a);
    } else {
      testErrObject(rs, Byte.class);
    }
    testErrObject(rs, byte[].class);
    testObject(rs, Boolean.class, true);
    testErrObject(rs, Clob.class);
    testErrObject(rs, NClob.class);
    testErrObject(rs, InputStream.class);
    testErrObject(rs, Reader.class);
    testObject(rs, LocalDate.class, LocalDate.parse("2010-01-01"));
    testObject(rs, LocalDateTime.class, LocalDateTime.parse("2010-01-01T00:00:00"));
    testErrObject(rs, LocalTime.class);
    testErrObject(rs, Time.class);
    testObject(rs, BigInteger.class, isMariaDBServer() ? BigInteger.TEN : BigInteger.valueOf(2010));
    testObject(rs, Timestamp.class, Timestamp.valueOf("2010-01-01 00:00:00"));
    testObject(
        rs,
        ZonedDateTime.class,
        LocalDateTime.parse("2010-01-01T00:00:00").atZone(ZoneId.systemDefault()));
    testObject(rs, java.util.Date.class, Date.valueOf("2010-01-01"));
    rs.next();

    testObject(rs, Integer.class, isMariaDBServer() ? 80 : 1980);
    testObject(rs, String.class, isMariaDBServer() ? "80" : "1980");
    testObject(rs, Long.class, isMariaDBServer() ? 80L : 1980L);
    testObject(rs, Short.class, (short) (isMariaDBServer() ? 80 : 1980));
    testObject(rs, BigDecimal.class, BigDecimal.valueOf(isMariaDBServer() ? 80 : 1980));
    testObject(rs, BigInteger.class, BigInteger.valueOf(isMariaDBServer() ? 80 : 1980));
    testObject(rs, Double.class, isMariaDBServer() ? 80d : 1980d);
    testObject(rs, Float.class, isMariaDBServer() ? 80f : 1980f);

    if (isMariaDBServer()) {
      testObject(rs, Byte.class, (byte) 80);
    } else {
      testErrObject(rs, Byte.class);
    }
    testErrObject(rs, byte[].class);
    testObject(rs, Boolean.class, true);
    testErrObject(rs, Clob.class);
    testErrObject(rs, NClob.class);
    testErrObject(rs, InputStream.class);
    testErrObject(rs, Reader.class);
    testObject(rs, LocalDate.class, LocalDate.parse("1980-01-01"));
    testObject(rs, LocalDateTime.class, LocalDateTime.parse("1980-01-01T00:00:00"));
    testErrObject(rs, LocalTime.class);
    testErrObject(rs, Time.class);
    testObject(
        rs,
        BigInteger.class,
        isMariaDBServer() ? BigInteger.valueOf(80) : BigInteger.valueOf(1980));
    testObject(rs, Timestamp.class, Timestamp.valueOf("1980-01-01 00:00:00"));
    testObject(
        rs,
        ZonedDateTime.class,
        LocalDateTime.parse("1980-01-01T00:00:00").atZone(ZoneId.systemDefault()));
    testObject(rs, java.util.Date.class, Date.valueOf("1980-01-01"));
  }

  @Test
  public void getString() throws SQLException {
    getString(get());
  }

  @Test
  public void getStringPrepare() throws SQLException {
    getString(getPrepare(sharedConn));
    getString(getPrepare(sharedConnBinary));
  }

  public void getString(ResultSet rs) throws SQLException {
    assertEquals(isMariaDBServer() ? "10" : "2010", rs.getString(1));
    assertFalse(rs.wasNull());
    assertEquals("1901", rs.getString(2));
    assertEquals("1901", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("2155", rs.getString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getString(4));
    assertTrue(rs.wasNull());
    rs.next();
    assertEquals(isMariaDBServer() ? "80" : "1980", rs.getString(1));
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
    assertEquals(isMariaDBServer() ? "10" : "2010", rs.getNString(1));
    assertFalse(rs.wasNull());
    assertEquals("1901", rs.getNString(2));
    assertEquals("1901", rs.getNString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("2155", rs.getNString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getNString(4));
    assertTrue(rs.wasNull());
    rs.next();
    assertEquals(isMariaDBServer() ? "80" : "1980", rs.getString(1));
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
    assertTrue(rs.getBoolean(1));
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
    if (isMariaDBServer()) {
      assertEquals(Byte.valueOf("10"), rs.getByte(1));
    } else {
      Common.assertThrowsContains(SQLDataException.class, () -> rs.getByte(1), "byte overflow");
    }
    assertFalse(rs.wasNull());
    Common.assertThrowsContains(SQLDataException.class, () -> rs.getByte(2), "byte overflow");
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getByte("t2alias"), "byte overflow");
    assertFalse(rs.wasNull());
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
    assertEquals(isMariaDBServer() ? 10 : 2010, rs.getShort(1));
    assertFalse(rs.wasNull());
    assertEquals(1901, rs.getShort(2));
    assertEquals(1901, rs.getShort("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(2155, rs.getShort(3));
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getShort(4));
    assertTrue(rs.wasNull());
    rs.next();
    assertEquals(isMariaDBServer() ? 80 : 1980, rs.getShort(1));
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
    assertEquals(isMariaDBServer() ? 10 : 2010, rs.getInt(1));
    assertFalse(rs.wasNull());
    assertEquals(1901, rs.getInt(2));
    assertEquals(1901, rs.getInt("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(2155, rs.getInt(3));
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getInt(4));
    assertTrue(rs.wasNull());
    rs.next();
    assertEquals(isMariaDBServer() ? 80 : 1980, rs.getInt(1));
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
    assertEquals(isMariaDBServer() ? 10 : 2010, rs.getLong(1));
    assertFalse(rs.wasNull());
    assertEquals(1901, rs.getLong(2));
    assertEquals(1901, rs.getLong("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(2155, rs.getLong(3));
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getLong(4));
    assertTrue(rs.wasNull());
    rs.next();
    assertEquals(isMariaDBServer() ? 80 : 1980, rs.getLong(1));
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
    assertEquals(isMariaDBServer() ? 10F : 2010f, rs.getFloat(1));
    assertFalse(rs.wasNull());
    assertEquals(1901F, rs.getFloat(2));
    assertEquals(1901F, rs.getFloat("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(2155F, rs.getFloat(3));
    assertFalse(rs.wasNull());
    assertEquals(0F, rs.getFloat(4));
    assertTrue(rs.wasNull());
    rs.next();
    assertEquals(isMariaDBServer() ? 80f : 1980f, rs.getFloat(1));
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
    assertEquals(isMariaDBServer() ? 10d : 2010d, rs.getDouble(1));
    assertFalse(rs.wasNull());
    assertEquals(1901D, rs.getDouble(2));
    assertEquals(1901D, rs.getDouble("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(2155D, rs.getDouble(3));
    assertFalse(rs.wasNull());
    assertEquals(0D, rs.getDouble(4));
    assertTrue(rs.wasNull());
    rs.next();
    assertEquals(isMariaDBServer() ? 80d : 1980d, rs.getDouble(1));
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

  public void getBigDecimal(ResultSet rs) throws SQLException {
    assertEquals(
        isMariaDBServer() ? BigDecimal.TEN : BigDecimal.valueOf(2010), rs.getBigDecimal(1));
    assertFalse(rs.wasNull());
    assertEquals(BigDecimal.valueOf(1901), rs.getBigDecimal(2));
    assertEquals(BigDecimal.valueOf(1901), rs.getBigDecimal("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(BigDecimal.valueOf(2155), rs.getBigDecimal(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBigDecimal(4));
    assertTrue(rs.wasNull());
    rs.next();
    assertEquals(
        isMariaDBServer() ? BigDecimal.valueOf(80) : BigDecimal.valueOf(1980), rs.getBigDecimal(1));
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

  public void getDate(ResultSet rs) throws SQLException {
    assertEquals(Date.valueOf("2010-01-01").getTime(), rs.getDate(1).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Date.valueOf("1901-01-01").getTime(), rs.getDate(2).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Date.valueOf("2155-01-01").getTime(), rs.getDate(3).getTime());
    assertFalse(rs.wasNull());
    assertNull(rs.getDate(4));
    assertTrue(rs.wasNull());
    rs.next();
    assertEquals(Date.valueOf("1980-01-01").getTime(), rs.getDate(1).getTime());
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
        SQLException.class, () -> rs.getTime(1), "Data type YEAR cannot be decoded as Time");
  }

  @Test
  public void getDuration() throws SQLException {
    getDuration(get());
  }

  @Test
  public void getDurationPrepare() throws SQLException {
    getDuration(getPrepare(sharedConn));
    getDuration(getPrepare(sharedConnBinary));
  }

  public void getDuration(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class,
        () -> rs.getObject(1, Duration.class),
        "Type class java.time.Duration not supported type for YEAR type");
  }

  @Test
  public void getLocalTime() throws SQLException {
    getLocalTime(get());
  }

  @Test
  public void getLocalTimePrepare() throws SQLException {
    getLocalTime(getPrepare(sharedConn));
    getLocalTime(getPrepare(sharedConnBinary));
  }

  public void getLocalTime(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class,
        () -> rs.getObject(1, LocalTime.class),
        "Type class java.time.LocalTime not supported type for YEAR type");
  }

  @Test
  public void getLocalDate() throws SQLException {
    getLocalDate(get());
  }

  @Test
  public void getLocalDatePrepare() throws SQLException {
    getLocalDate(getPrepare(sharedConn));
    getLocalDate(getPrepare(sharedConnBinary));
  }

  public void getLocalDate(ResultSet rs) throws SQLException {
    assertEquals(LocalDate.parse("2010-01-01"), rs.getObject(1, LocalDate.class));
    assertFalse(rs.wasNull());
    assertEquals(LocalDate.parse("1901-01-01"), rs.getObject(2, LocalDate.class));
    assertEquals(LocalDate.parse("1901-01-01"), rs.getObject("t2alias", LocalDate.class));
    assertFalse(rs.wasNull());
    assertEquals(LocalDate.parse("2155-01-01"), rs.getObject(3, LocalDate.class));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4, LocalDate.class));
    assertTrue(rs.wasNull());
    rs.next();
    assertEquals(LocalDate.parse("1980-01-01"), rs.getObject(1, LocalDate.class));
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

  public void getTimestamp(ResultSet rs) throws SQLException {
    assertEquals(Timestamp.valueOf("2010-01-01 00:00:00").getTime(), rs.getTimestamp(1).getTime());
    assertEquals(
        1262304000000L,
        rs.getTimestamp(1, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertEquals(
        Timestamp.valueOf("2010-01-01 00:00:00").getTime(), rs.getTimestamp("t1alias").getTime());
    assertEquals(
        1262304000000L,
        rs.getTimestamp("t1alias", Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Timestamp.valueOf("1901-01-01 00:00:00").getTime(), rs.getTimestamp(2).getTime());
    assertEquals(
        -2177452800000L,
        rs.getTimestamp(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());

    assertEquals(Timestamp.valueOf("2155-01-01 00:00:00").getTime(), rs.getTimestamp(3).getTime());
    assertEquals(
        5838048000000L,
        rs.getTimestamp(3, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Timestamp.valueOf("2155-01-01 00:00:00"), rs.getTimestamp(3));
    assertNull(rs.getTimestamp(4));
    assertTrue(rs.wasNull());
    rs.next();
    assertEquals(Timestamp.valueOf("1980-01-01 00:00:00").getTime(), rs.getTimestamp(1).getTime());
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
        SQLException.class,
        () -> rs.getAsciiStream(1),
        "Data type YEAR cannot be decoded as Stream");
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
        SQLException.class,
        () -> rs.getUnicodeStream(1),
        "Data type YEAR cannot be decoded as Stream");
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
        SQLException.class,
        () -> rs.getBinaryStream(1),
        "Data type YEAR cannot be decoded as Stream");
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
        SQLException.class, () -> rs.getBytes(1), "Data type YEAR cannot be decoded as byte[]");
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
        SQLException.class,
        () -> rs.getCharacterStream(1),
        "Data type YEAR cannot be decoded as Reader");
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
        "Data type YEAR cannot be decoded as Reader");
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
        SQLException.class, () -> rs.getBlob(1), "Data type YEAR cannot be decoded as Blob");
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
        SQLException.class, () -> rs.getClob(1), "Data type YEAR cannot be decoded as Clob");
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
        SQLException.class, () -> rs.getNClob(1), "Data type YEAR cannot be decoded as Clob");
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = get();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("YEAR", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.sql.Date", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.DATE, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(isMariaDBServer() ? 2 : 4, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(isMariaDBServer() ? 2 : 4, meta.getColumnDisplaySize(1));
  }
}
