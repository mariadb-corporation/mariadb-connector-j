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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.result.CompleteResult;
import org.mariadb.jdbc.integration.Common;

public class MediumIntCodecTest extends CommonCodecTest {
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS MediumIntCodec");
    stmt.execute("DROP TABLE IF EXISTS MediumIntCodec2");
    stmt.execute("DROP TABLE IF EXISTS MediumIntCodec3");
    stmt.execute("DROP TABLE IF EXISTS MediumIntCodecUnsigned");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE MediumIntCodec (t1 MEDIUMINT, t2 MEDIUMINT, t3 MEDIUMINT, t4 MEDIUMINT)");
    stmt.execute(
        "CREATE TABLE MediumIntCodecUnsigned (t1 MEDIUMINT UNSIGNED, t2 MEDIUMINT UNSIGNED, t3"
            + " MEDIUMINT UNSIGNED, t4 MEDIUMINT UNSIGNED)");
    stmt.execute("INSERT INTO MediumIntCodec VALUES (0, 1, -1, null)");
    stmt.execute("INSERT INTO MediumIntCodecUnsigned VALUES (0, 1, 16777215, null)");
    stmt.execute(
        "CREATE TABLE MediumIntCodec2 (id int not null primary key auto_increment, t1 MEDIUMINT)");
    stmt.execute(
        "CREATE TABLE MediumIntCodec3 (id int not null, id2 int not null, t1 MEDIUMINT, PRIMARY KEY"
            + " (id, id2))");
    stmt.execute("FLUSH TABLES");
  }

  private ResultSet getSigned() throws SQLException {
    return get("MediumIntCodec");
  }

  private ResultSet getUnsigned() throws SQLException {
    return get("MediumIntCodecUnsigned");
  }

  private ResultSet getSignedPrepared(Connection con) throws SQLException {
    return getPrepare(con, "MediumIntCodec");
  }

  private ResultSet getUnsignedPrepared(Connection con) throws SQLException {
    return getPrepare(con, "MediumIntCodecUnsigned");
  }

  private ResultSet get(String table) throws SQLException {
    Statement stmt =
        sharedConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from " + table);
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private ResultSet getPrepare(Connection con, String table) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    PreparedStatement preparedStatement =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from "
                + table
                + " WHERE 1 > ?",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
    preparedStatement.closeOnCompletion();
    preparedStatement.setInt(1, 0);
    ResultSet rs = preparedStatement.executeQuery();
    assertTrue(rs.next());
    con.commit();
    return rs;
  }

  @Test
  public void getObject() throws SQLException {
    getObject(getSigned());
  }

  @Test
  public void getObjectPrepared() throws SQLException {
    getObject(getSignedPrepared(sharedConn));
    getObject(getSignedPrepared(sharedConnBinary));
  }

  private void getObject(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getObject(1));
    assertFalse(rs.wasNull());
    assertEquals(1, rs.getObject(2));
    assertEquals(1, rs.getObject("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(-1, rs.getObject(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getObjectType() throws Exception {
    getObjectType(getSigned());
  }

  @Test
  public void getObjectTypePrepared() throws Exception {
    getObjectType(getSignedPrepared(sharedConn));
    getObjectType(getSignedPrepared(sharedConnBinary));
  }

  private void getObjectType(ResultSet rs) throws Exception {
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
    getString(getSigned());
  }

  @Test
  public void getStringPrepared() throws SQLException {
    getString(getSignedPrepared(sharedConn));
    getString(getSignedPrepared(sharedConnBinary));
  }

  private void getString(ResultSet rs) throws SQLException {
    assertEquals("0", rs.getString(1));
    assertFalse(rs.wasNull());
    assertEquals("1", rs.getString(2));
    assertEquals("1", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("-1", rs.getString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getString(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getStringUnsigned() throws SQLException {
    getStringUnsigned(getUnsigned());
  }

  @Test
  public void getStringUnsignedPrepared() throws SQLException {
    getStringUnsigned(getUnsignedPrepared(sharedConn));
    getStringUnsigned(getUnsignedPrepared(sharedConnBinary));
  }

  private void getStringUnsigned(ResultSet rs) throws SQLException {
    assertEquals("0", rs.getString(1));
    assertFalse(rs.wasNull());
    assertEquals("1", rs.getString(2));
    assertEquals("1", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("16777215", rs.getString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getString(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getNString() throws SQLException {
    getNString(getSigned());
  }

  @Test
  public void getNStringPrepared() throws SQLException {
    getNString(getSignedPrepared(sharedConn));
    getNString(getSignedPrepared(sharedConnBinary));
  }

  private void getNString(ResultSet rs) throws SQLException {
    assertEquals("0", rs.getNString(1));
    assertFalse(rs.wasNull());
    assertEquals("1", rs.getNString(2));
    assertEquals("1", rs.getNString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("-1", rs.getNString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getNString(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBoolean() throws SQLException {
    getBoolean(getSigned());
  }

  @Test
  public void getBooleanPrepared() throws SQLException {
    getBoolean(getSignedPrepared(sharedConn));
    getBoolean(getSignedPrepared(sharedConnBinary));
  }

  private void getBoolean(ResultSet rs) throws SQLException {
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
    getByte(getSigned());
  }

  @Test
  public void getBytePrepared() throws SQLException {
    getByte(getSignedPrepared(sharedConn));
    getByte(getSignedPrepared(sharedConnBinary));
  }

  private void getByte(ResultSet rs) throws SQLException {
    assertEquals((byte) 0, rs.getByte(1));
    assertFalse(rs.wasNull());
    assertEquals((byte) 1, rs.getByte(2));
    assertEquals((byte) 1, rs.getByte("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals((byte) -1, rs.getByte(3));
    assertFalse(rs.wasNull());
    assertEquals((byte) 0, rs.getByte(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getShort() throws SQLException {
    getShort(getSigned());
  }

  @Test
  public void getShortPrepared() throws SQLException {
    getShort(getSignedPrepared(sharedConn));
    getShort(getSignedPrepared(sharedConnBinary));
  }

  private void getShort(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getShort(1));
    assertFalse(rs.wasNull());
    assertEquals(1, rs.getShort(2));
    assertEquals(1, rs.getShort("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(-1, rs.getShort(3));
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getShort(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getInt() throws SQLException {
    getInt(getSigned());
  }

  @Test
  public void getIntPrepared() throws SQLException {
    getInt(getSignedPrepared(sharedConn));
    getInt(getSignedPrepared(sharedConnBinary));
  }

  private void getInt(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getInt(1));
    assertFalse(rs.wasNull());
    assertEquals(1, rs.getInt(2));
    assertEquals(1, rs.getInt("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(-1, rs.getInt(3));
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getInt(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getIntUnsigned() throws SQLException {
    getIntUnsigned(getUnsigned());
  }

  @Test
  public void getIntUnsignedPrepared() throws SQLException {
    getIntUnsigned(getUnsignedPrepared(sharedConn));
    getIntUnsigned(getUnsignedPrepared(sharedConnBinary));
  }

  private void getIntUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getInt(1));
    assertFalse(rs.wasNull());
    assertEquals(1, rs.getInt(2));
    assertFalse(rs.wasNull());
    assertEquals(16777215, rs.getInt(3));
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getInt(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getLong() throws SQLException {
    getLong(getSigned());
  }

  @Test
  public void getLongPrepared() throws SQLException {
    getLong(getSignedPrepared(sharedConn));
    getLong(getSignedPrepared(sharedConnBinary));
  }

  private void getLong(ResultSet rs) throws SQLException {
    assertEquals(0L, rs.getLong(1));
    assertFalse(rs.wasNull());
    assertEquals(1L, rs.getLong(2));
    assertEquals(1L, rs.getLong("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(-1L, rs.getLong(3));
    assertFalse(rs.wasNull());
    assertEquals(0L, rs.getLong(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getLongUnsigned() throws SQLException {
    getLongUnsigned(getUnsigned());
  }

  @Test
  public void getLongUnsignedPrepared() throws SQLException {
    getLongUnsigned(getUnsignedPrepared(sharedConn));
    getLongUnsigned(getUnsignedPrepared(sharedConnBinary));
  }

  private void getLongUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0L, rs.getLong(1));
    assertFalse(rs.wasNull());
    assertEquals(1L, rs.getLong(2));
    assertEquals(1L, rs.getLong("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(16777215L, rs.getLong(3));
    assertFalse(rs.wasNull());
    assertEquals(0L, rs.getLong(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getFloat() throws SQLException {
    getFloat(getSigned());
  }

  @Test
  public void getFloatPrepared() throws SQLException {
    getFloat(getSignedPrepared(sharedConn));
    getFloat(getSignedPrepared(sharedConnBinary));
  }

  private void getFloat(ResultSet rs) throws SQLException {
    assertEquals(0F, rs.getFloat(1));
    assertFalse(rs.wasNull());
    assertEquals(1F, rs.getFloat(2));
    assertEquals(1F, rs.getFloat("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(-1F, rs.getFloat(3));
    assertFalse(rs.wasNull());
    assertEquals(0F, rs.getFloat(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getFloatUnsigned() throws SQLException {
    getFloatUnsigned(getUnsigned());
  }

  @Test
  public void getFloatUnsignedPrepared() throws SQLException {
    getFloatUnsigned(getUnsignedPrepared(sharedConn));
    getFloatUnsigned(getUnsignedPrepared(sharedConnBinary));
  }

  private void getFloatUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0F, rs.getFloat(1));
    assertFalse(rs.wasNull());
    assertEquals(1F, rs.getFloat(2));
    assertEquals(1F, rs.getFloat("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(16777215F, rs.getFloat(3));
    assertFalse(rs.wasNull());
    assertEquals(0F, rs.getFloat(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDouble() throws SQLException {
    getDouble(getSigned());
  }

  @Test
  public void getDoublePrepared() throws SQLException {
    getDouble(getSignedPrepared(sharedConn));
    getDouble(getSignedPrepared(sharedConnBinary));
  }

  private void getDouble(ResultSet rs) throws SQLException {
    assertEquals(0D, rs.getDouble(1));
    assertFalse(rs.wasNull());
    assertEquals(1D, rs.getDouble(2));
    assertEquals(1D, rs.getDouble("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(-1D, rs.getDouble(3));
    assertFalse(rs.wasNull());
    assertEquals(0D, rs.getDouble(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDoubleUnsigned() throws SQLException {
    getDoubleUnsigned(getUnsigned());
  }

  @Test
  public void getDoubleUnsignedPrepared() throws SQLException {
    getDoubleUnsigned(getUnsignedPrepared(sharedConn));
    getDoubleUnsigned(getUnsignedPrepared(sharedConnBinary));
  }

  private void getDoubleUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0D, rs.getDouble(1));
    assertFalse(rs.wasNull());
    assertEquals(1D, rs.getDouble(2));
    assertEquals(1D, rs.getDouble("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(16777215D, rs.getDouble(3));
    assertFalse(rs.wasNull());
    assertEquals(0D, rs.getDouble(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBigDecimal() throws SQLException {
    getBigDecimal(getSigned());
  }

  @Test
  public void getBigDecimalPrepared() throws SQLException {
    getBigDecimal(getSignedPrepared(sharedConn));
    getBigDecimal(getSignedPrepared(sharedConnBinary));
  }

  private void getBigDecimal(ResultSet rs) throws SQLException {
    assertEquals(BigDecimal.ZERO, rs.getBigDecimal(1));
    assertFalse(rs.wasNull());
    assertEquals(BigDecimal.ONE, rs.getBigDecimal(2));
    assertEquals(BigDecimal.ONE, rs.getBigDecimal("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(BigDecimal.valueOf(-1), rs.getBigDecimal(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBigDecimal(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBigDecimalUnsigned() throws SQLException {
    getBigDecimalUnsigned(getUnsigned());
  }

  @Test
  public void getBigDecimalUnsignedPrepared() throws SQLException {
    getBigDecimalUnsigned(getUnsignedPrepared(sharedConn));
    getBigDecimalUnsigned(getUnsignedPrepared(sharedConnBinary));
  }

  private void getBigDecimalUnsigned(ResultSet rs) throws SQLException {
    assertEquals(BigDecimal.ZERO, rs.getBigDecimal(1));
    assertFalse(rs.wasNull());
    assertEquals(BigDecimal.ONE, rs.getBigDecimal(2));
    assertEquals(BigDecimal.ONE, rs.getBigDecimal("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(BigDecimal.valueOf(16777215L), rs.getBigDecimal(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBigDecimal(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBigInteger() throws SQLException {
    getBigInteger(getSigned());
  }

  @Test
  public void getBigIntegerPrepared() throws SQLException {
    getBigInteger(getSignedPrepared(sharedConn));
    getBigInteger(getSignedPrepared(sharedConnBinary));
  }

  private void getBigInteger(ResultSet res) throws SQLException {
    CompleteResult rs = (CompleteResult) res;
    assertEquals(BigInteger.ZERO, rs.getBigInteger(1));
    assertFalse(rs.wasNull());
    assertEquals(BigInteger.ONE, rs.getBigInteger(2));
    assertEquals(BigInteger.ONE, rs.getBigInteger("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(BigInteger.valueOf(-1), rs.getBigInteger(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBigInteger(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBigIntegerUnsigned() throws SQLException {
    getBigIntegerUnsigned(getUnsigned());
  }

  @Test
  public void getBigIntegerUnsignedPrepared() throws SQLException {
    getBigIntegerUnsigned(getUnsignedPrepared(sharedConn));
    getBigIntegerUnsigned(getUnsignedPrepared(sharedConnBinary));
  }

  private void getBigIntegerUnsigned(ResultSet res) throws SQLException {
    CompleteResult rs = (CompleteResult) res;
    assertEquals(BigInteger.ZERO, rs.getBigInteger(1));
    assertFalse(rs.wasNull());
    assertEquals(BigInteger.ONE, rs.getBigInteger(2));
    assertEquals(BigInteger.ONE, rs.getBigInteger("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(BigInteger.valueOf(16777215L), rs.getBigInteger(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBigInteger(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDate() throws SQLException {
    getDate(getSigned());
  }

  @Test
  public void getDatePrepared() throws SQLException {
    getDate(getSignedPrepared(sharedConn));
    getDate(getSignedPrepared(sharedConnBinary));
  }

  private void getDate(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getDate(1),
        "Data type MEDIUMINT cannot be decoded as Date");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getDate("t1alias"),
        "Data type MEDIUMINT cannot be decoded as Date");
  }

  @Test
  public void getTime() throws SQLException {
    getTime(getSigned());
  }

  @Test
  public void getTimePrepared() throws SQLException {
    getTime(getSignedPrepared(sharedConn));
    getTime(getSignedPrepared(sharedConnBinary));
  }

  private void getTime(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTime(1),
        "Data type MEDIUMINT cannot be decoded as Time");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTime("t1alias"),
        "Data type MEDIUMINT cannot be decoded as Time");
  }

  @Test
  public void getTimestamp() throws SQLException {
    getTimestamp(getSigned());
  }

  @Test
  public void getTimestampPrepared() throws SQLException {
    getTimestamp(getSignedPrepared(sharedConn));
    getTimestamp(getSignedPrepared(sharedConnBinary));
  }

  private void getTimestamp(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp(1),
        "Data type MEDIUMINT cannot be decoded as Timestamp");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp("t1alias"),
        "Data type MEDIUMINT cannot be decoded as Timestamp");
  }

  @Test
  public void getAsciiStream() throws SQLException {
    getAsciiStream(getSigned());
  }

  @Test
  public void getAsciiStreamPrepared() throws SQLException {
    getAsciiStream(getSignedPrepared(sharedConn));
    getAsciiStream(getSignedPrepared(sharedConnBinary));
  }

  private void getAsciiStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getAsciiStream(1),
        "Data type MEDIUMINT cannot be decoded as Stream");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getAsciiStream("t1alias"),
        "Data type MEDIUMINT cannot be decoded as Stream");
  }

  @Test
  public void getUnicodeStream() throws SQLException {
    getUnicodeStream(getSigned());
  }

  @Test
  public void getUnicodeStreamPrepared() throws SQLException {
    getUnicodeStream(getSignedPrepared(sharedConn));
    getUnicodeStream(getSignedPrepared(sharedConnBinary));
  }

  @SuppressWarnings("deprecation")
  private void getUnicodeStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getUnicodeStream(1),
        "Data type MEDIUMINT cannot be decoded as Stream");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getUnicodeStream("t1alias"),
        "Data type MEDIUMINT cannot be decoded as Stream");
  }

  @Test
  public void getBinaryStream() throws SQLException {
    getBinaryStream(getSigned());
  }

  @Test
  public void getBinaryStreamPrepared() throws SQLException {
    getBinaryStream(getSignedPrepared(sharedConn));
    getBinaryStream(getSignedPrepared(sharedConnBinary));
  }

  private void getBinaryStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBinaryStream(1),
        "Data type MEDIUMINT cannot be decoded as Stream");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBinaryStream("t1alias"),
        "Data type MEDIUMINT cannot be decoded as Stream");
  }

  @Test
  public void getBytes() throws SQLException {
    getBytes(getSigned());
  }

  @Test
  public void getBytesPrepared() throws SQLException {
    getBytes(getSignedPrepared(sharedConn));
    getBytes(getSignedPrepared(sharedConnBinary));
  }

  private void getBytes(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBytes(1),
        "Data type MEDIUMINT cannot be decoded as byte[]");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBytes("t1alias"),
        "Data type MEDIUMINT cannot be decoded as byte[]");
  }

  @Test
  public void getCharacterStream() throws SQLException {
    getCharacterStream(getSigned());
  }

  @Test
  public void getCharacterStreamPrepared() throws SQLException {
    getCharacterStream(getSignedPrepared(sharedConn));
    getCharacterStream(getSignedPrepared(sharedConnBinary));
  }

  private void getCharacterStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type MEDIUMINT cannot be decoded as Reader");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream("t1alias"),
        "Data type MEDIUMINT cannot be decoded as Reader");
  }

  @Test
  public void getNCharacterStream() throws SQLException {
    getNCharacterStream(getSigned());
  }

  @Test
  public void getNCharacterStreamPrepared() throws SQLException {
    getNCharacterStream(getSignedPrepared(sharedConn));
    getNCharacterStream(getSignedPrepared(sharedConnBinary));
  }

  private void getNCharacterStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream(1),
        "Data type MEDIUMINT cannot be decoded as Reader");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream("t2alias"),
        "Data type MEDIUMINT cannot be decoded as Reader");
  }

  @Test
  public void getRef() throws SQLException {
    getRef(getSigned());
  }

  @Test
  public void getRefPrepared() throws SQLException {
    getRef(getSignedPrepared(sharedConn));
    getRef(getSignedPrepared(sharedConnBinary));
  }

  private void getRef(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class, () -> rs.getRef(1), "Method ResultSet.getRef not supported");
    Common.assertThrowsContains(
        SQLException.class, () -> rs.getRef("t2alias"), "Method ResultSet.getRef not supported");
  }

  @Test
  public void getBlob() throws SQLException {
    getBlob(getSigned());
  }

  @Test
  public void getBlobPrepared() throws SQLException {
    getBlob(getSignedPrepared(sharedConn));
    getBlob(getSignedPrepared(sharedConnBinary));
  }

  private void getBlob(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type MEDIUMINT cannot be decoded as Reader");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream("t1alias"),
        "Data type MEDIUMINT cannot be decoded as Reader");
  }

  @Test
  public void getClob() throws SQLException {
    getClob(getSigned());
  }

  @Test
  public void getClobPrepared() throws SQLException {
    getClob(getSignedPrepared(sharedConn));
    getClob(getSignedPrepared(sharedConnBinary));
  }

  private void getClob(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getClob(1),
        "Data type MEDIUMINT cannot be decoded as Clob");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getClob("t1alias"),
        "Data type MEDIUMINT cannot be decoded as Clob");
  }

  @Test
  public void getNClob() throws SQLException {
    getNClob(getSigned());
  }

  @Test
  public void getNClobPrepared() throws SQLException {
    getNClob(getSignedPrepared(sharedConn));
    getNClob(getSignedPrepared(sharedConnBinary));
  }

  private void getNClob(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNClob(1),
        "Data type MEDIUMINT cannot be decoded as Clob");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNClob("t1alias"),
        "Data type MEDIUMINT cannot be decoded as Clob");
  }

  @Test
  public void getArray() throws SQLException {
    getArray(getSigned());
  }

  @Test
  public void getArrayPrepared() throws SQLException {
    getArray(getSignedPrepared(sharedConn));
    getArray(getSignedPrepared(sharedConnBinary));
  }

  private void getArray(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class,
        () -> rs.getArray(1),
        "Data type MEDIUMINT cannot be decoded as float[]");
    Common.assertThrowsContains(
        SQLException.class,
        () -> rs.getArray("t1alias"),
        "Data type MEDIUMINT cannot be decoded as float[]");
  }

  @Test
  public void getURL() throws SQLException {
    getURL(getSigned());
  }

  @Test
  public void getURLPrepared() throws SQLException {
    getURL(getSignedPrepared(sharedConn));
    getURL(getSignedPrepared(sharedConnBinary));
  }

  private void getURL(ResultSet rs) {
    Common.assertThrowsContains(
        SQLSyntaxErrorException.class, () -> rs.getURL(1), "Could not parse '0' as URL");
    Common.assertThrowsContains(
        SQLSyntaxErrorException.class, () -> rs.getURL("t1alias"), "Could not parse '0' as URL");
  }

  @Test
  public void getSQLXML() throws SQLException {
    getSQLXML(getSigned());
  }

  @Test
  public void getSQLXMLPrepared() throws SQLException {
    getSQLXML(getSignedPrepared(sharedConn));
    getSQLXML(getSignedPrepared(sharedConnBinary));
  }

  private void getSQLXML(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class, () -> rs.getSQLXML(1), "Method ResultSet.getSQLXML not supported");
    Common.assertThrowsContains(
        SQLException.class,
        () -> rs.getSQLXML("t1alias"),
        "Method ResultSet.getSQLXML not supported");
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = getSigned();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("MEDIUMINT", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.lang.Integer", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.INTEGER, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(7, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(8, meta.getColumnDisplaySize(1));
  }

  @Test
  public void sendParam() throws SQLException {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE MediumIntCodec3");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO MediumIntCodec3(id, id2, t1) VALUES (?,?,?)")) {
      prep.setInt(1, 1);
      prep.setInt(2, 1);
      prep.setInt(3, 3);
      prep.execute();
      prep.setInt(1, 2);
      prep.setInt(2, 1);
      prep.setObject(3, 2);
      prep.execute();
      prep.setInt(1, 3);
      prep.setInt(2, 1);
      prep.setObject(3, null);
      prep.execute();
      prep.setInt(1, 4);
      prep.setInt(2, 1);
      prep.setObject(3, 3, Types.INTEGER);
      prep.execute();
      prep.setInt(1, 5);
      prep.setInt(2, 1);
      prep.setObject(3, null, Types.INTEGER);
      prep.execute();
    }
    ResultSet rs =
        con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM MediumIntCodec3");
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(3));
    rs.updateInt("t1", 10);
    rs.updateRow();
    assertEquals(10, rs.getInt(3));

    assertTrue(rs.next());
    assertEquals(2, rs.getInt(3));
    rs.updateObject("t1", null);
    rs.updateRow();
    assertEquals(0, rs.getInt(3));

    assertTrue(rs.next());
    assertEquals(0, rs.getInt(3));
    assertTrue(rs.wasNull());
    rs.updateInt(3, 50);
    rs.updateRow();
    assertEquals(50, rs.getInt(3));

    assertTrue(rs.next());
    assertEquals(3, rs.getInt(3));
    rs.updateObject(3, null, Types.INTEGER);
    rs.updateRow();
    assertEquals(0, rs.getInt(3));
    assertTrue(rs.wasNull());

    assertTrue(rs.next());
    assertEquals(0, rs.getInt(3));
    assertTrue(rs.wasNull());
    rs.updateObject(3, 85, Types.INTEGER);
    rs.updateRow();
    assertEquals(85, rs.getInt(3));
    assertFalse(rs.wasNull());

    rs = stmt.executeQuery("SELECT * FROM MediumIntCodec3");
    assertTrue(rs.next());
    assertEquals(10, rs.getInt(3));

    assertTrue(rs.next());
    assertEquals(0, rs.getInt(3));

    assertTrue(rs.next());
    assertEquals(50, rs.getInt(3));

    assertTrue(rs.next());
    assertEquals(0, rs.getInt(3));
    assertTrue(rs.wasNull());

    assertTrue(rs.next());
    assertEquals(85, rs.getInt(3));
    assertFalse(rs.wasNull());
    con.commit();
  }
}
