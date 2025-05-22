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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.result.CompleteResult;
import org.mariadb.jdbc.integration.Common;

public class NullCodecTest extends CommonCodecTest {

  private ResultSet getSigned() throws SQLException {
    return get("IntCodec");
  }

  private ResultSet getUnsigned() throws SQLException {
    return get("IntCodecUnsigned");
  }

  private ResultSet get(String table) throws SQLException {
    Statement stmt =
        sharedConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs = stmt.executeQuery("select NULL as t1alias");
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private ResultSet getPreparedSigned(Connection con) throws SQLException {
    return getPrepare(con, "IntCodec");
  }

  private ResultSet getPreparedUnsigned(Connection con) throws SQLException {
    return getPrepare(con, "IntCodecUnsigned");
  }

  private ResultSet getPrepare(Connection con, String table) throws SQLException {
    Assumptions.assumeTrue(
        (isMariaDBServer() && minVersion(10, 4, 0)) || (!isMariaDBServer() && minVersion(8, 0, 0)));
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement preparedStatement =
        con.prepareStatement(
            "select NULL as t1alias WHERE 1 > ?",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)) {
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
    getObject(getSigned());
  }

  @Test
  public void getObjectPrepared() throws SQLException {
    getObject(getPreparedSigned(sharedConn));
    getObject(getPreparedSigned(sharedConnBinary));
  }

  private void getObject(ResultSet rs) throws SQLException {
    assertNull(rs.getObject(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getObject("t1alias"));
    assertTrue(rs.wasNull());
    Common.assertThrowsContains(
        SQLException.class,
        () -> rs.getObject(1, int.class),
        "Cannot return null for primitive int");
  }

  @Test
  public void getObjectType() throws Exception {
    getObjectType(getSigned());
  }

  @Test
  public void getObjectTypePrepared() throws Exception {
    getObjectType(getPreparedSigned(sharedConn));
    getObjectType(getPreparedSigned(sharedConnBinary));
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
    getString(getSigned());
  }

  @Test
  public void getStringPrepared() throws SQLException {
    getString(getPreparedSigned(sharedConn));
    getString(getPreparedSigned(sharedConnBinary));
  }

  private void getString(ResultSet rs) throws SQLException {
    assertNull(rs.getString(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getString("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getStringUnsigned() throws SQLException {
    getStringUnsigned(getUnsigned());
  }

  @Test
  public void getStringUnsignedPrepared() throws SQLException {
    getStringUnsigned(getPreparedUnsigned(sharedConn));
    getStringUnsigned(getPreparedUnsigned(sharedConnBinary));
  }

  private void getStringUnsigned(ResultSet rs) throws SQLException {
    assertNull(rs.getString(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getString("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getNString() throws SQLException {
    getNString(getSigned());
  }

  @Test
  public void getNStringPrepared() throws SQLException {
    getNString(getPreparedSigned(sharedConn));
    getNString(getPreparedSigned(sharedConnBinary));
  }

  private void getNString(ResultSet rs) throws SQLException {
    assertNull(rs.getNString(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getNString("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBoolean() throws SQLException {
    getBoolean(getSigned());
  }

  @Test
  public void getBooleanPrepared() throws SQLException {
    getBoolean(getPreparedSigned(sharedConn));
    getBoolean(getPreparedSigned(sharedConnBinary));
  }

  private void getBoolean(ResultSet rs) throws SQLException {
    assertFalse(rs.getBoolean(1));
    assertTrue(rs.wasNull());
    assertFalse(rs.getBoolean("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getByte() throws SQLException {
    getByte(getSigned());
  }

  @Test
  public void getBytePrepared() throws SQLException {
    getByte(getPreparedSigned(sharedConn));
    getByte(getPreparedSigned(sharedConnBinary));
  }

  private void getByte(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getByte(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getByte("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getUnsignedByte() throws SQLException {
    getUnsignedByte(getUnsigned());
  }

  @Test
  public void getUnsignedBytePrepared() throws SQLException {
    getUnsignedByte(getPreparedUnsigned(sharedConn));
    getUnsignedByte(getPreparedUnsigned(sharedConnBinary));
  }

  private void getUnsignedByte(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getByte(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getByte("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getShort() throws SQLException {
    getShort(getSigned());
  }

  @Test
  public void getShortPrepared() throws SQLException {
    getShort(getPreparedSigned(sharedConn));
    getShort(getPreparedSigned(sharedConnBinary));
  }

  private void getShort(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getShort(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getShort("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getInt() throws SQLException {
    getInt(getSigned());
  }

  @Test
  public void getIntPrepared() throws SQLException {
    getInt(getPreparedSigned(sharedConn));
    getInt(getPreparedSigned(sharedConnBinary));
  }

  private void getInt(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getInt(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getInt("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getIntUnsigned() throws SQLException {
    getIntUnsigned(getUnsigned());
  }

  @Test
  public void getIntUnsignedPrepared() throws SQLException {
    getIntUnsigned(getPreparedUnsigned(sharedConn));
    getIntUnsigned(getPreparedUnsigned(sharedConnBinary));
  }

  private void getIntUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getInt(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getInt("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getLong() throws SQLException {
    getLong(getSigned());
  }

  @Test
  public void getLongPrepared() throws SQLException {
    getLong(getPreparedSigned(sharedConn));
    getLong(getPreparedSigned(sharedConnBinary));
  }

  private void getLong(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getLong(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getLong("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getLongUnsigned() throws SQLException {
    getLongUnsigned(getUnsigned());
  }

  @Test
  public void getLongUnsignedPrepared() throws SQLException {
    getLongUnsigned(getPreparedUnsigned(sharedConn));
    getLongUnsigned(getPreparedUnsigned(sharedConnBinary));
  }

  private void getLongUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getLong(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getLong("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getFloat() throws SQLException {
    getFloat(getSigned());
  }

  @Test
  public void getFloatPrepared() throws SQLException {
    getFloat(getPreparedSigned(sharedConn));
    getFloat(getPreparedSigned(sharedConnBinary));
  }

  private void getFloat(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getFloat(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getFloat("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getFloatUnsigned() throws SQLException {
    getFloatUnsigned(getUnsigned());
  }

  @Test
  public void getFloatUnsignedPrepared() throws SQLException {
    getFloatUnsigned(getPreparedUnsigned(sharedConn));
    getFloatUnsigned(getPreparedUnsigned(sharedConnBinary));
  }

  private void getFloatUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getFloat(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getFloat("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDouble() throws SQLException {
    getDouble(getSigned());
  }

  @Test
  public void getDoublePrepared() throws SQLException {
    getDouble(getPreparedSigned(sharedConn));
    getDouble(getPreparedSigned(sharedConnBinary));
  }

  private void getDouble(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getDouble(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getDouble("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDoubleUnsigned() throws SQLException {
    getDoubleUnsigned(getUnsigned());
  }

  @Test
  public void getDoubleUnsignedPrepared() throws SQLException {
    getDoubleUnsigned(getPreparedUnsigned(sharedConn));
    getDoubleUnsigned(getPreparedUnsigned(sharedConnBinary));
  }

  private void getDoubleUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getDouble(1));
    assertTrue(rs.wasNull());
    assertEquals(0, rs.getDouble("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBigDecimal() throws SQLException {
    getBigDecimal(getSigned());
  }

  @Test
  public void getBigDecimalPrepared() throws SQLException {
    getBigDecimal(getPreparedSigned(sharedConn));
    getBigDecimal(getPreparedSigned(sharedConnBinary));
  }

  private void getBigDecimal(ResultSet rs) throws SQLException {
    assertNull(rs.getBigDecimal(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getBigDecimal("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBigDecimalUnsigned() throws SQLException {
    getBigDecimalUnsigned(getUnsigned());
  }

  @Test
  public void getBigDecimalUnsignedPrepared() throws SQLException {
    getBigDecimalUnsigned(getPreparedUnsigned(sharedConn));
    getBigDecimalUnsigned(getPreparedUnsigned(sharedConnBinary));
  }

  private void getBigDecimalUnsigned(ResultSet rs) throws SQLException {
    assertNull(rs.getBigDecimal(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getBigDecimal("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBigInteger() throws SQLException {
    getBigInteger(getSigned());
  }

  @Test
  public void getBigIntegerPrepared() throws SQLException {
    getBigInteger(getPreparedSigned(sharedConn));
    getBigInteger(getPreparedSigned(sharedConnBinary));
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
    getBigIntegerUnsigned(getUnsigned());
  }

  @Test
  public void getBigIntegerUnsignedPrepared() throws SQLException {
    getBigIntegerUnsigned(getPreparedUnsigned(sharedConn));
    getBigIntegerUnsigned(getPreparedUnsigned(sharedConnBinary));
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
    getDuration(getSigned());
  }

  @Test
  public void getDurationPrepare() throws SQLException {
    getDuration(getPreparedSigned(sharedConn));
    getDuration(getPreparedSigned(sharedConnBinary));
  }

  public void getDuration(ResultSet rs) throws SQLException {
    assertNull(rs.getObject(1, Duration.class));
    assertTrue(rs.wasNull());
    assertNull(rs.getObject("t1alias", Duration.class));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDate() throws SQLException {
    getDate(getSigned());
  }

  @Test
  public void getDatePrepared() throws SQLException {
    getDate(getPreparedSigned(sharedConn));
    getDate(getPreparedSigned(sharedConnBinary));
  }

  private void getDate(ResultSet rs) throws SQLException {
    assertNull(rs.getDate(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getDate("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getTime() throws SQLException {
    getTime(getSigned());
  }

  @Test
  public void getTimePrepared() throws SQLException {
    getTime(getPreparedSigned(sharedConn));
    getTime(getPreparedSigned(sharedConnBinary));
  }

  private void getTime(ResultSet rs) throws SQLException {
    assertNull(rs.getTime(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getTime("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getTimestamp() throws SQLException {
    getTimestamp(getSigned());
  }

  @Test
  public void getTimestampPrepared() throws SQLException {
    getTimestamp(getPreparedSigned(sharedConn));
    getTimestamp(getPreparedSigned(sharedConnBinary));
  }

  private void getTimestamp(ResultSet rs) throws SQLException {
    assertNull(rs.getTimestamp(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getTimestamp("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getAsciiStream() throws SQLException {
    getAsciiStream(getSigned());
  }

  @Test
  public void getAsciiStreamPrepared() throws SQLException {
    getAsciiStream(getPreparedSigned(sharedConn));
    getAsciiStream(getPreparedSigned(sharedConnBinary));
  }

  private void getAsciiStream(ResultSet rs) throws SQLException {
    assertNull(rs.getAsciiStream(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getAsciiStream("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getUnicodeStream() throws SQLException {
    getUnicodeStream(getSigned());
  }

  @Test
  public void getUnicodeStreamPrepared() throws SQLException {
    getUnicodeStream(getPreparedSigned(sharedConn));
    getUnicodeStream(getPreparedSigned(sharedConnBinary));
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
    getBinaryStream(getSigned());
  }

  @Test
  public void getBinaryStreamPrepared() throws SQLException {
    getBinaryStream(getPreparedSigned(sharedConn));
    getBinaryStream(getPreparedSigned(sharedConnBinary));
  }

  private void getBinaryStream(ResultSet rs) throws SQLException {
    assertNull(rs.getBinaryStream(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getBinaryStream("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBytes() throws SQLException {
    getBytes(getSigned());
  }

  @Test
  public void getBytesPrepared() throws SQLException {
    getBytes(getPreparedSigned(sharedConn));
    getBytes(getPreparedSigned(sharedConnBinary));
  }

  private void getBytes(ResultSet rs) throws SQLException {
    assertNull(rs.getBytes(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getBytes("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getCharacterStream() throws SQLException {
    getCharacterStream(getSigned());
  }

  @Test
  public void getCharacterStreamPrepared() throws SQLException {
    getCharacterStream(getPreparedSigned(sharedConn));
    getCharacterStream(getPreparedSigned(sharedConnBinary));
  }

  private void getCharacterStream(ResultSet rs) throws SQLException {
    assertNull(rs.getCharacterStream(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getCharacterStream("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getNCharacterStream() throws SQLException {
    getNCharacterStream(getSigned());
  }

  @Test
  public void getNCharacterStreamPrepared() throws SQLException {
    getNCharacterStream(getPreparedSigned(sharedConn));
    getNCharacterStream(getPreparedSigned(sharedConnBinary));
  }

  private void getNCharacterStream(ResultSet rs) throws SQLException {
    assertNull(rs.getNCharacterStream(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getNCharacterStream("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBlob() throws SQLException {
    getBlob(getSigned());
  }

  @Test
  public void getBlobPrepared() throws SQLException {
    getBlob(getPreparedSigned(sharedConn));
    getBlob(getPreparedSigned(sharedConnBinary));
  }

  private void getBlob(ResultSet rs) throws SQLException {
    assertNull(rs.getCharacterStream(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getCharacterStream("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getClob() throws SQLException {
    getClob(getSigned());
  }

  @Test
  public void getClobPrepared() throws SQLException {
    getClob(getPreparedSigned(sharedConn));
    getClob(getPreparedSigned(sharedConnBinary));
  }

  private void getClob(ResultSet rs) throws SQLException {
    assertNull(rs.getClob(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getClob("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getNClob() throws SQLException {
    getNClob(getSigned());
  }

  @Test
  public void getNClobPrepared() throws SQLException {
    getNClob(getPreparedSigned(sharedConn));
    getNClob(getPreparedSigned(sharedConnBinary));
  }

  private void getNClob(ResultSet rs) throws SQLException {
    assertNull(rs.getNClob(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getNClob("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getURL() throws SQLException {
    getURL(getSigned());
  }

  @Test
  public void getURLPrepared() throws SQLException {
    getURL(getPreparedSigned(sharedConn));
    getURL(getPreparedSigned(sharedConnBinary));
  }

  private void getURL(ResultSet rs) throws SQLException {
    assertNull(rs.getURL(1));
    assertTrue(rs.wasNull());
    assertNull(rs.getURL("t1alias"));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = getSigned();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("NULL", meta.getColumnTypeName(1));
    assertEquals("", meta.getCatalogName(1));
    assertEquals("byte[]", meta.getColumnClassName(1));
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
