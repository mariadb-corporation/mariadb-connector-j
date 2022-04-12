// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2022 SingleStore, Inc.

package com.singlestore.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.result.CompleteResult;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class NullCodecTest extends CommonCodecTest {
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS NullCodec");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE IF NOT EXISTS NullCodec (t1 int)");
    stmt.execute("INSERT INTO NullCodec VALUES (1)");
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs = stmt.executeQuery("select NULL as t1alias");
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private ResultSet getPrepared(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement preparedStatement =
        con.prepareStatement("select NULL as t1alias from NullCodec WHERE 1 > ?")) {
      preparedStatement.setInt(1, 0);
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      return rs;
    } finally {
      con.commit();
    }
  }

  @Test
  public void getObject() throws SQLException {
    getObject(get());
  }

  @Test
  public void getObjectPrepared() throws SQLException {
    getObject(getPrepared(sharedConn));
    getObject(getPrepared(sharedConnBinary));
  }

  private void getObject(ResultSet rs) throws SQLException {
    assertNull(rs.getObject(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getObject("t1alias"));
    assertTrue(rs.wasNull());
    assertThrowsContains(
        SQLException.class,
        () -> rs.getObject(1, int.class),
        "Cannot return null for primitive int");
  }

  @Test
  public void getObjectType() throws Exception {
    getObjectType(get());
  }

  @Test
  public void getObjectTypePrepared() throws Exception {
    getObjectType(getPrepared(sharedConn));
    getObjectType(getPrepared(sharedConnBinary));
  }

  void testNullObject(ResultSet rs, Class<?> objClass) throws Exception {
    assertNull(rs.getObject(1, objClass));
    assertNull(rs.getObject("t1alias", objClass));
  }

  private void getObjectType(ResultSet rs) throws Exception {
    testNullObject(rs, Integer.class);
    testNullObject(rs, String.class);
    testNullObject(rs, Long.class);
    testNullObject(rs, Short.class);
    testNullObject(rs, BigDecimal.class);
    testNullObject(rs, BigInteger.class);
    testNullObject(rs, Double.class);
    testNullObject(rs, Float.class);
    testNullObject(rs, Byte.class);
    testNullObject(rs, byte[].class);
    testNullObject(rs, Date.class);
    testNullObject(rs, Time.class);
    testNullObject(rs, Timestamp.class);
    testNullObject(rs, java.util.Date.class);
    testNullObject(rs, LocalDate.class);
    testNullObject(rs, ZonedDateTime.class);
    testNullObject(rs, OffsetDateTime.class);
    testNullObject(rs, LocalDateTime.class);
    testNullObject(rs, OffsetTime.class);
    testNullObject(rs, Boolean.class);
    testNullObject(rs, Clob.class);
    testNullObject(rs, NClob.class);
    testNullObject(rs, InputStream.class);
    testNullObject(rs, Reader.class);
    rs.next();
  }

  @Test
  public void getString() throws SQLException {
    getString(get());
  }

  @Test
  public void getStringPrepared() throws SQLException {
    getString(getPrepared(sharedConn));
    getString(getPrepared(sharedConnBinary));
  }

  private void getString(ResultSet rs) throws SQLException {
    assertNull(rs.getString(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getString("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getStringUnsigned() throws SQLException {
    getStringUnsigned(get());
  }

  @Test
  public void getStringUnsignedPrepared() throws SQLException {
    getStringUnsigned(getPrepared(sharedConn));
    getStringUnsigned(getPrepared(sharedConnBinary));
  }

  private void getStringUnsigned(ResultSet rs) throws SQLException {
    assertNull(rs.getString(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getString("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getNString() throws SQLException {
    getNString(get());
  }

  @Test
  public void getNStringPrepared() throws SQLException {
    getNString(getPrepared(sharedConn));
    getNString(getPrepared(sharedConnBinary));
  }

  private void getNString(ResultSet rs) throws SQLException {
    assertNull(rs.getNString(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getNString("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBoolean() throws SQLException {
    getBoolean(get());
  }

  @Test
  public void getBooleanPrepared() throws SQLException {
    getBoolean(getPrepared(sharedConn));
    getBoolean(getPrepared(sharedConnBinary));
  }

  private void getBoolean(ResultSet rs) throws SQLException {
    assertEquals(false, rs.getBoolean(1));
    assertTrue(rs.wasNull());
    assertEquals(false, rs.getBoolean("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getByte() throws SQLException {
    getByte(get());
  }

  @Test
  public void getBytePrepared() throws SQLException {
    getByte(getPrepared(sharedConn));
    getByte(getPrepared(sharedConnBinary));
  }

  private void getByte(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getByte(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getByte("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getShort() throws SQLException {
    getShort(get());
  }

  @Test
  public void getShortPrepared() throws SQLException {
    getShort(getPrepared(sharedConn));
    getShort(getPrepared(sharedConnBinary));
  }

  private void getShort(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getShort(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getShort("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getInt() throws SQLException {
    getInt(get());
  }

  @Test
  public void getIntPrepared() throws SQLException {
    getInt(getPrepared(sharedConn));
    getInt(getPrepared(sharedConnBinary));
  }

  private void getInt(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getInt(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getInt("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getIntUnsigned() throws SQLException {
    getIntUnsigned(get());
  }

  @Test
  public void getIntUnsignedPrepared() throws SQLException {
    getIntUnsigned(getPrepared(sharedConn));
    getIntUnsigned(getPrepared(sharedConnBinary));
  }

  private void getIntUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getInt(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getInt("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getLong() throws SQLException {
    getLong(get());
  }

  @Test
  public void getLongPrepared() throws SQLException {
    getLong(getPrepared(sharedConn));
    getLong(getPrepared(sharedConnBinary));
  }

  private void getLong(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getLong(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getLong("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getLongUnsigned() throws SQLException {
    getLongUnsigned(get());
  }

  @Test
  public void getLongUnsignedPrepared() throws SQLException {
    getLongUnsigned(getPrepared(sharedConn));
    getLongUnsigned(getPrepared(sharedConnBinary));
  }

  private void getLongUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getLong(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getLong("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getFloat() throws SQLException {
    getFloat(get());
  }

  @Test
  public void getFloatPrepared() throws SQLException {
    getFloat(getPrepared(sharedConn));
    getFloat(getPrepared(sharedConnBinary));
  }

  private void getFloat(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getFloat(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getFloat("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getFloatUnsigned() throws SQLException {
    getFloatUnsigned(get());
  }

  @Test
  public void getFloatUnsignedPrepared() throws SQLException {
    getFloatUnsigned(getPrepared(sharedConn));
    getFloatUnsigned(getPrepared(sharedConnBinary));
  }

  private void getFloatUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getFloat(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getFloat("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDouble() throws SQLException {
    getDouble(get());
  }

  @Test
  public void getDoublePrepared() throws SQLException {
    getDouble(getPrepared(sharedConn));
    getDouble(getPrepared(sharedConnBinary));
  }

  private void getDouble(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getDouble(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getDouble("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDoubleUnsigned() throws SQLException {
    getDoubleUnsigned(get());
  }

  @Test
  public void getDoubleUnsignedPrepared() throws SQLException {
    getDoubleUnsigned(getPrepared(sharedConn));
    getDoubleUnsigned(getPrepared(sharedConnBinary));
  }

  private void getDoubleUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getDouble(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getDouble("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBigDecimal() throws SQLException {
    getBigDecimal(get());
  }

  @Test
  public void getBigDecimalPrepared() throws SQLException {
    getBigDecimal(getPrepared(sharedConn));
    getBigDecimal(getPrepared(sharedConnBinary));
  }

  private void getBigDecimal(ResultSet rs) throws SQLException {
    assertNull(rs.getBigDecimal(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getBigDecimal("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBigDecimalUnsigned() throws SQLException {
    getBigDecimalUnsigned(get());
  }

  @Test
  public void getBigDecimalUnsignedPrepared() throws SQLException {
    getBigDecimalUnsigned(getPrepared(sharedConn));
    getBigDecimalUnsigned(getPrepared(sharedConnBinary));
  }

  private void getBigDecimalUnsigned(ResultSet rs) throws SQLException {
    assertNull(rs.getBigDecimal(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getBigDecimal("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBigInteger() throws SQLException {
    getBigInteger(get());
  }

  @Test
  public void getBigIntegerPrepared() throws SQLException {
    getBigInteger(getPrepared(sharedConn));
    getBigInteger(getPrepared(sharedConnBinary));
  }

  private void getBigInteger(ResultSet res) throws SQLException {
    CompleteResult rs = (CompleteResult) res;
    assertNull(rs.getBigInteger(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getBigInteger("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBigIntegerUnsigned() throws SQLException {
    getBigIntegerUnsigned(get());
  }

  @Test
  public void getBigIntegerUnsignedPrepared() throws SQLException {
    getBigIntegerUnsigned(getPrepared(sharedConn));
    getBigIntegerUnsigned(getPrepared(sharedConnBinary));
  }

  private void getBigIntegerUnsigned(ResultSet res) throws SQLException {
    CompleteResult rs = (CompleteResult) res;
    assertNull(rs.getBigInteger(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getBigInteger("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDuration() throws SQLException {
    getDuration(get());
  }

  @Test
  public void getDurationPrepare() throws SQLException {
    getDuration(getPrepared(sharedConn));
    getDuration(getPrepared(sharedConnBinary));
  }

  public void getDuration(ResultSet rs) throws SQLException {
    assertNull(rs.getObject(1, Duration.class));
    assertTrue(rs.wasNull());
    assertNull(rs.getObject("t1alias", Duration.class));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDate() throws SQLException {
    getDate(get());
  }

  @Test
  public void getDatePrepared() throws SQLException {
    getDate(getPrepared(sharedConn));
    getDate(getPrepared(sharedConnBinary));
  }

  private void getDate(ResultSet rs) throws SQLException {
    assertNull(rs.getDate(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getDate("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getTime() throws SQLException {
    getTime(get());
  }

  @Test
  public void getTimePrepared() throws SQLException {
    getTime(getPrepared(sharedConn));
    getTime(getPrepared(sharedConnBinary));
  }

  private void getTime(ResultSet rs) throws SQLException {
    assertNull(rs.getTime(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getTime("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getTimestamp() throws SQLException {
    getTimestamp(get());
  }

  @Test
  public void getTimestampPrepared() throws SQLException {
    getTimestamp(getPrepared(sharedConn));
    getTimestamp(getPrepared(sharedConnBinary));
  }

  private void getTimestamp(ResultSet rs) throws SQLException {
    assertNull(rs.getTimestamp(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getTimestamp("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getAsciiStream() throws SQLException {
    getAsciiStream(get());
  }

  @Test
  public void getAsciiStreamPrepared() throws SQLException {
    getAsciiStream(getPrepared(sharedConn));
    getAsciiStream(getPrepared(sharedConnBinary));
  }

  private void getAsciiStream(ResultSet rs) throws SQLException {
    assertNull(rs.getAsciiStream(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getAsciiStream("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getUnicodeStream() throws SQLException {
    getUnicodeStream(get());
  }

  @Test
  public void getUnicodeStreamPrepared() throws SQLException {
    getUnicodeStream(getPrepared(sharedConn));
    getUnicodeStream(getPrepared(sharedConnBinary));
  }

  @SuppressWarnings("deprecation")
  private void getUnicodeStream(ResultSet rs) throws SQLException {
    assertNull(rs.getUnicodeStream(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getUnicodeStream("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBinaryStream() throws SQLException {
    getBinaryStream(get());
  }

  @Test
  public void getBinaryStreamPrepared() throws SQLException {
    getBinaryStream(getPrepared(sharedConn));
    getBinaryStream(getPrepared(sharedConnBinary));
  }

  private void getBinaryStream(ResultSet rs) throws SQLException {
    assertNull(rs.getBinaryStream(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getBinaryStream("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBytes() throws SQLException {
    getBytes(get());
  }

  @Test
  public void getBytesPrepared() throws SQLException {
    getBytes(getPrepared(sharedConn));
    getBytes(getPrepared(sharedConnBinary));
  }

  private void getBytes(ResultSet rs) throws SQLException {
    assertNull(rs.getBytes(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getBytes("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getCharacterStream() throws SQLException {
    getCharacterStream(get());
  }

  @Test
  public void getCharacterStreamPrepared() throws SQLException {
    getCharacterStream(getPrepared(sharedConn));
    getCharacterStream(getPrepared(sharedConnBinary));
  }

  private void getCharacterStream(ResultSet rs) throws SQLException {
    assertNull(rs.getCharacterStream(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getCharacterStream("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getNCharacterStream() throws SQLException {
    getNCharacterStream(get());
  }

  @Test
  public void getNCharacterStreamPrepared() throws SQLException {
    getNCharacterStream(getPrepared(sharedConn));
    getNCharacterStream(getPrepared(sharedConnBinary));
  }

  private void getNCharacterStream(ResultSet rs) throws SQLException {
    assertNull(rs.getNCharacterStream(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getNCharacterStream("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBlob() throws SQLException {
    getBlob(get());
  }

  @Test
  public void getBlobPrepared() throws SQLException {
    getBlob(getPrepared(sharedConn));
    getBlob(getPrepared(sharedConnBinary));
  }

  private void getBlob(ResultSet rs) throws SQLException {
    assertNull(rs.getCharacterStream(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getCharacterStream("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getClob() throws SQLException {
    getClob(get());
  }

  @Test
  public void getClobPrepared() throws SQLException {
    getClob(getPrepared(sharedConn));
    getClob(getPrepared(sharedConnBinary));
  }

  private void getClob(ResultSet rs) throws SQLException {
    assertNull(rs.getClob(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getClob("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getNClob() throws SQLException {
    getNClob(get());
  }

  @Test
  public void getNClobPrepared() throws SQLException {
    getNClob(getPrepared(sharedConn));
    getNClob(getPrepared(sharedConnBinary));
  }

  private void getNClob(ResultSet rs) throws SQLException {
    assertNull(rs.getNClob(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getNClob("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getURL() throws SQLException {
    getURL(get());
  }

  @Test
  public void getURLPrepared() throws SQLException {
    getURL(getPrepared(sharedConn));
    getURL(getPrepared(sharedConnBinary));
  }

  private void getURL(ResultSet rs) throws SQLException {
    assertNull(rs.getURL(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getURL("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = get();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("NULL", meta.getColumnTypeName(1));
    assertEquals("", meta.getCatalogName(1));
    assertEquals("java.lang.String", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1alias", meta.getColumnName(1));
    assertEquals(Types.NULL, meta.getColumnType(1));
    assertEquals(1, meta.getColumnCount());
    assertEquals(0, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(0, meta.getColumnDisplaySize(1));
  }
}
