// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
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

public class TinyIntCodecTest extends CommonCodecTest {
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS TinyIntCodec");
    stmt.execute("DROP TABLE IF EXISTS TinyIntCodec2");
    stmt.execute("DROP TABLE IF EXISTS TinyIntCodecUnsigned");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE TinyIntCodec (t1 TINYINT, t2 TINYINT, t3 TINYINT, t4 TINYINT)");
    stmt.execute(
        "CREATE TABLE TinyIntCodec2 (id int not null primary key auto_increment, t1 TINYINT)");
    stmt.execute(
        "CREATE TABLE TinyIntCodecUnsigned (t1 TINYINT UNSIGNED, t2 TINYINT UNSIGNED, t3 TINYINT"
            + " UNSIGNED, t4 TINYINT UNSIGNED)");
    stmt.execute("INSERT INTO TinyIntCodec VALUES (0, 1, -1, null)");
    stmt.execute("INSERT INTO TinyIntCodecUnsigned VALUES (0, 1, 255, null)");
    stmt.execute("FLUSH TABLES");
  }

  private ResultSet getSigned() throws SQLException {
    return get("TinyIntCodec");
  }

  private ResultSet getUnsigned() throws SQLException {
    return get("TinyIntCodecUnsigned");
  }

  private ResultSet getPrepareSigned(Connection con) throws SQLException {
    return getPrepare(con, "TinyIntCodec");
  }

  private ResultSet getPrepareUnsigned(Connection con) throws SQLException {
    return getPrepare(con, "TinyIntCodecUnsigned");
  }

  private ResultSet get(String table) throws SQLException {
    Statement stmt = sharedConn.createStatement();
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
                + " WHERE 1 > ?");
    preparedStatement.closeOnCompletion();
    preparedStatement.setInt(1, 0);
    ResultSet rs = preparedStatement.executeQuery();
    assertTrue(rs.next());
    con.commit();
    return rs;
  }

  @Test
  public void getObject() throws Exception {
    getObject(getSigned());
  }

  @Test
  public void getObjectPrepare() throws Exception {
    getObject(getPrepareSigned(sharedConn));
    getObject(getPrepareSigned(sharedConnBinary));
  }

  public void getObject(ResultSet rs) throws Exception {
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
  public void getObjectUnsigned() throws Exception {
    getObjectUnsigned(getUnsigned());
  }

  @Test
  public void getObjectUnsignedPrepare() throws Exception {
    getObjectUnsigned(getPrepareUnsigned(sharedConn));
    getObjectUnsigned(getPrepareUnsigned(sharedConnBinary));
  }

  public void getObjectUnsigned(ResultSet rs) throws Exception {
    assertEquals(0, rs.getObject(1));
    assertFalse(rs.wasNull());
    assertEquals(1, rs.getObject(2));
    assertEquals(1, rs.getObject("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(255, rs.getObject(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getObjectType() throws Exception {
    getObjectType(getSigned());
  }

  @Test
  public void getObjectTypePrepare() throws Exception {
    getObjectType(getPrepareSigned(sharedConn));
    getObjectType(getPrepareSigned(sharedConnBinary));
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
  public void getObjectTypeBoolean() throws Exception {
    getObjectTypeBoolean(getSigned());
  }

  @Test
  public void getObjectTypeBooleanPrepare() throws Exception {
    getObjectTypeBoolean(getPrepareSigned(sharedConn));
    getObjectTypeBoolean(getPrepareSigned(sharedConnBinary));
  }

  public void getObjectTypeBoolean(ResultSet rs) throws Exception {
    assertEquals(Boolean.FALSE, rs.getObject(1, Boolean.class));
    assertFalse(rs.wasNull());
    assertEquals(Boolean.TRUE, rs.getObject(2, Boolean.class));
    assertEquals(Boolean.TRUE, rs.getObject("t2alias", Boolean.class));
    assertFalse(rs.wasNull());
    assertEquals(Boolean.TRUE, rs.getObject(3, Boolean.class));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4, Boolean.class));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getString() throws Exception {
    getString(getSigned());
  }

  @Test
  public void getStringPrepare() throws Exception {
    getString(getPrepareSigned(sharedConn));
    getString(getPrepareSigned(sharedConnBinary));
  }

  public void getString(ResultSet rs) throws Exception {
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
    getStringUnsigned(getPrepareUnsigned(sharedConn));
    getStringUnsigned(getPrepareUnsigned(sharedConnBinary));
  }

  private void getStringUnsigned(ResultSet rs) throws SQLException {
    assertEquals("0", rs.getString(1));
    assertFalse(rs.wasNull());
    assertEquals("1", rs.getString(2));
    assertEquals("1", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("255", rs.getString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getString(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getNString() throws Exception {
    getNString(getSigned());
  }

  @Test
  public void getNStringPrepare() throws Exception {
    getNString(getPrepareSigned(sharedConn));
    getNString(getPrepareSigned(sharedConnBinary));
  }

  public void getNString(ResultSet rs) throws Exception {
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
  public void getBoolean() throws Exception {
    getBoolean(getSigned());
  }

  @Test
  public void getBooleanPrepare() throws Exception {
    getBoolean(getPrepareSigned(sharedConn));
    getBoolean(getPrepareSigned(sharedConnBinary));
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
  public void getByte() throws SQLException {
    getByte(getSigned());
  }

  @Test
  public void getBytePrepare() throws SQLException {
    getByte(getPrepareSigned(sharedConn));
    getByte(getPrepareSigned(sharedConnBinary));
  }

  public void getByte(ResultSet rs) throws SQLException {
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
  public void getShortPrepare() throws SQLException {
    getShort(getPrepareSigned(sharedConn));
    getShort(getPrepareSigned(sharedConnBinary));
  }

  public void getShort(ResultSet rs) throws SQLException {
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
  public void getIntPrepare() throws SQLException {
    getInt(getPrepareSigned(sharedConn));
    getInt(getPrepareSigned(sharedConnBinary));
  }

  public void getInt(ResultSet rs) throws SQLException {
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
  public void getIntUnsignedPrepare() throws SQLException {
    getIntUnsigned(getPrepareUnsigned(sharedConn));
    getIntUnsigned(getPrepareUnsigned(sharedConnBinary));
  }

  public void getIntUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getInt(1));
    assertFalse(rs.wasNull());
    assertEquals(1, rs.getInt(2));
    assertFalse(rs.wasNull());
    assertEquals(255, rs.getInt(3));
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getInt(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getLong() throws SQLException {
    getLong(getSigned());
  }

  @Test
  public void getLongPrepare() throws SQLException {
    getLong(getPrepareSigned(sharedConn));
    getLong(getPrepareSigned(sharedConnBinary));
  }

  public void getLong(ResultSet rs) throws SQLException {
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
  public void getLongUnsignedPrepare() throws SQLException {
    getLongUnsigned(getPrepareUnsigned(sharedConn));
    getLongUnsigned(getPrepareUnsigned(sharedConnBinary));
  }

  public void getLongUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0L, rs.getLong(1));
    assertFalse(rs.wasNull());
    assertEquals(1L, rs.getLong(2));
    assertEquals(1L, rs.getLong("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(255L, rs.getLong(3));
    assertFalse(rs.wasNull());
    assertEquals(0L, rs.getLong(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getFloat() throws SQLException {
    getFloat(getSigned());
  }

  @Test
  public void getFloatPrepare() throws SQLException {
    getFloat(getPrepareSigned(sharedConn));
    getFloat(getPrepareSigned(sharedConnBinary));
  }

  public void getFloat(ResultSet rs) throws SQLException {
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
  public void getFloatUnsignedPrepare() throws SQLException {
    getFloatUnsigned(getPrepareUnsigned(sharedConn));
    getFloatUnsigned(getPrepareUnsigned(sharedConnBinary));
  }

  public void getFloatUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0F, rs.getFloat(1));
    assertFalse(rs.wasNull());
    assertEquals(1F, rs.getFloat(2));
    assertEquals(1F, rs.getFloat("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(255F, rs.getFloat(3));
    assertFalse(rs.wasNull());
    assertEquals(0F, rs.getFloat(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDouble() throws SQLException {
    getDouble(getSigned());
  }

  @Test
  public void getDoublePrepare() throws SQLException {
    getDouble(getPrepareSigned(sharedConn));
    getDouble(getPrepareSigned(sharedConnBinary));
  }

  public void getDouble(ResultSet rs) throws SQLException {
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
  public void getDoubleUnsignedPrepare() throws SQLException {
    getDoubleUnsigned(getPrepareUnsigned(sharedConn));
    getDoubleUnsigned(getPrepareUnsigned(sharedConnBinary));
  }

  public void getDoubleUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0D, rs.getDouble(1));
    assertFalse(rs.wasNull());
    assertEquals(1D, rs.getDouble(2));
    assertEquals(1D, rs.getDouble("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(255D, rs.getDouble(3));
    assertFalse(rs.wasNull());
    assertEquals(0D, rs.getDouble(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBigDecimal() throws SQLException {
    getBigDecimal(getSigned());
  }

  @Test
  public void getBigDecimalPrepare() throws SQLException {
    getBigDecimal(getPrepareSigned(sharedConn));
    getBigDecimal(getPrepareSigned(sharedConnBinary));
  }

  public void getBigDecimal(ResultSet rs) throws SQLException {
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
  public void getBigDecimalUnsignedPrepare() throws SQLException {
    getBigDecimalUnsigned(getPrepareUnsigned(sharedConn));
    getBigDecimalUnsigned(getPrepareUnsigned(sharedConnBinary));
  }

  public void getBigDecimalUnsigned(ResultSet rs) throws SQLException {
    assertEquals(BigDecimal.ZERO, rs.getBigDecimal(1));
    assertFalse(rs.wasNull());
    assertEquals(BigDecimal.ONE, rs.getBigDecimal(2));
    assertEquals(BigDecimal.ONE, rs.getBigDecimal("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(BigDecimal.valueOf(255L), rs.getBigDecimal(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBigDecimal(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBigIntegerUnsigned() throws SQLException {
    getBigIntegerUnsigned(getUnsigned());
  }

  @Test
  public void getBigIntegerUnsignedPrepare() throws SQLException {
    getBigIntegerUnsigned(getPrepareUnsigned(sharedConn));
    getBigIntegerUnsigned(getPrepareUnsigned(sharedConnBinary));
  }

  public void getBigIntegerUnsigned(ResultSet res) throws SQLException {
    CompleteResult rs = (CompleteResult) res;
    assertEquals(BigInteger.ZERO, rs.getBigInteger(1));
    assertFalse(rs.wasNull());
    assertEquals(BigInteger.ONE, rs.getBigInteger(2));
    assertEquals(BigInteger.ONE, rs.getBigInteger("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(BigInteger.valueOf(255L), rs.getBigInteger(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBigInteger(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDate() throws SQLException {
    getDate(getSigned());
  }

  @Test
  public void getDatePrepare() throws SQLException {
    getDate(getPrepareSigned(sharedConn));
    getDate(getPrepareSigned(sharedConnBinary));
  }

  public void getDate(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getDate(1), "Data type TINYINT cannot be decoded as Date");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getDate("t1alias"),
        "Data type TINYINT cannot be decoded as Date");
  }

  @Test
  public void getTime() throws SQLException {
    getTime(getSigned());
  }

  @Test
  public void getTimePrepare() throws SQLException {
    getTime(getPrepareSigned(sharedConn));
    getTime(getPrepareSigned(sharedConnBinary));
  }

  public void getTime(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getTime(1), "Data type TINYINT cannot be decoded as Time");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTime("t1alias"),
        "Data type TINYINT cannot be decoded as Time");
  }

  @Test
  public void getTimestamp() throws SQLException {
    getTimestamp(getSigned());
  }

  @Test
  public void getTimestampPrepare() throws SQLException {
    getTimestamp(getPrepareSigned(sharedConn));
    getTimestamp(getPrepareSigned(sharedConnBinary));
  }

  public void getTimestamp(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp(1),
        "Data type TINYINT cannot be decoded as Timestamp");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp("t1alias"),
        "Data type TINYINT cannot be decoded as Timestamp");
  }

  @Test
  public void getAsciiStream() throws SQLException {
    getAsciiStream(getSigned());
  }

  @Test
  public void getAsciiStreamPrepare() throws SQLException {
    getAsciiStream(getPrepareSigned(sharedConn));
    getAsciiStream(getPrepareSigned(sharedConnBinary));
  }

  public void getAsciiStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getAsciiStream(1),
        "Data type TINYINT cannot be decoded as Stream");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getAsciiStream("t1alias"),
        "Data type TINYINT cannot be decoded as Stream");
  }

  @Test
  public void getUnicodeStream() throws SQLException {
    getUnicodeStream(getSigned());
  }

  @Test
  public void getUnicodeStreamPrepare() throws SQLException {
    getUnicodeStream(getPrepareSigned(sharedConn));
    getUnicodeStream(getPrepareSigned(sharedConnBinary));
  }

  @SuppressWarnings("deprecation")
  public void getUnicodeStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getUnicodeStream(1),
        "Data type TINYINT cannot be decoded as Stream");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getUnicodeStream("t1alias"),
        "Data type TINYINT cannot be decoded as Stream");
  }

  @Test
  public void getBinaryStream() throws SQLException {
    getBinaryStream(getSigned());
  }

  @Test
  public void getBinaryStreamPrepare() throws SQLException {
    getBinaryStream(getPrepareSigned(sharedConn));
    getBinaryStream(getPrepareSigned(sharedConnBinary));
  }

  public void getBinaryStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBinaryStream(1),
        "Data type TINYINT cannot be decoded as Stream");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBinaryStream("t1alias"),
        "Data type TINYINT cannot be decoded as Stream");
  }

  @Test
  public void getBytes() throws SQLException {
    getBytes(getSigned());
  }

  @Test
  public void getBytesPrepare() throws SQLException {
    getBytes(getPrepareSigned(sharedConn));
    getBytes(getPrepareSigned(sharedConnBinary));
  }

  public void getBytes(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBytes(1),
        "Data type TINYINT cannot be decoded as byte[]");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBytes("t1alias"),
        "Data type TINYINT cannot be decoded as byte[]");
  }

  @Test
  public void getCharacterStream() throws SQLException {
    getCharacterStream(getSigned());
  }

  @Test
  public void getCharacterStreamPrepare() throws SQLException {
    getCharacterStream(getPrepareSigned(sharedConn));
    getCharacterStream(getPrepareSigned(sharedConnBinary));
  }

  public void getCharacterStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type TINYINT cannot be decoded as Reader");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream("t1alias"),
        "Data type TINYINT cannot be decoded as Reader");
  }

  @Test
  public void getNCharacterStream() throws SQLException {
    getNCharacterStream(getSigned());
  }

  @Test
  public void getNCharacterStreamPrepare() throws SQLException {
    getNCharacterStream(getPrepareSigned(sharedConn));
    getNCharacterStream(getPrepareSigned(sharedConnBinary));
  }

  public void getNCharacterStream(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream(1),
        "Data type TINYINT cannot be decoded as Reader");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream("t2alias"),
        "Data type TINYINT cannot be decoded as Reader");
  }

  @Test
  public void getRef() throws SQLException {
    getRef(getSigned());
  }

  @Test
  public void getRefPrepare() throws SQLException {
    getRef(getPrepareSigned(sharedConn));
    getRef(getPrepareSigned(sharedConnBinary));
  }

  public void getRef(ResultSet rs) {
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
  public void getBlobPrepare() throws SQLException {
    getBlob(getPrepareSigned(sharedConn));
    getBlob(getPrepareSigned(sharedConnBinary));
  }

  public void getBlob(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type TINYINT cannot be decoded as Reader");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream("t1alias"),
        "Data type TINYINT cannot be decoded as Reader");
  }

  @Test
  public void getClob() throws SQLException {
    getClob(getSigned());
  }

  @Test
  public void getClobPrepare() throws SQLException {
    getClob(getPrepareSigned(sharedConn));
    getClob(getPrepareSigned(sharedConnBinary));
  }

  public void getClob(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getClob(1), "Data type TINYINT cannot be decoded as Clob");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getClob("t1alias"),
        "Data type TINYINT cannot be decoded as Clob");
  }

  @Test
  public void getNClob() throws SQLException {
    getNClob(getSigned());
  }

  @Test
  public void getNClobPrepare() throws SQLException {
    getNClob(getPrepareSigned(sharedConn));
    getNClob(getPrepareSigned(sharedConnBinary));
  }

  public void getNClob(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNClob(1),
        "Data type TINYINT cannot be decoded as Clob");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNClob("t1alias"),
        "Data type TINYINT cannot be decoded as Clob");
  }

  @Test
  public void getArray() throws SQLException {
    getArray(getSigned());
  }

  @Test
  public void getArrayPrepare() throws SQLException {
    getArray(getPrepareSigned(sharedConn));
    getArray(getPrepareSigned(sharedConnBinary));
  }

  public void getArray(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class, () -> rs.getArray(1), "Method ResultSet.getArray not supported");
    Common.assertThrowsContains(
        SQLException.class,
        () -> rs.getArray("t1alias"),
        "Method ResultSet.getArray not supported");
  }

  @Test
  public void getURL() throws SQLException {
    getURL(getSigned());
  }

  @Test
  public void getURLPrepare() throws SQLException {
    getURL(getPrepareSigned(sharedConn));
    getURL(getPrepareSigned(sharedConnBinary));
  }

  public void getURL(ResultSet rs) {
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
  public void getSQLXMLPrepare() throws SQLException {
    getSQLXML(getPrepareSigned(sharedConn));
    getSQLXML(getPrepareSigned(sharedConnBinary));
  }

  public void getSQLXML(ResultSet rs) {
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
    assertEquals("TINYINT", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.lang.Integer", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.TINYINT, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(4, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(4, meta.getColumnDisplaySize(1));

    rs = getUnsigned();
    meta = rs.getMetaData();
    assertEquals("TINYINT UNSIGNED", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.lang.Integer", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.SMALLINT, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    // https://jira.mariadb.org/browse/XPT-276
    if (!isXpand()) {
      assertEquals(3, meta.getPrecision(1));
      assertEquals(3, meta.getColumnDisplaySize(1));
    }
  }

  @Test
  public void sendParam() throws SQLException {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE TinyIntCodec2");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO TinyIntCodec2(t1) VALUES (?)")) {
      prep.setByte(1, (byte) 1);
      prep.execute();
      prep.setBoolean(1, true);
      prep.execute();
      prep.setBoolean(1, false);
      prep.execute();
      prep.setBoolean(1, true);
      prep.execute();
      prep.setBoolean(1, false);
      prep.execute();
      prep.setObject(1, Byte.valueOf("2"));
      prep.execute();
      prep.setObject(1, null);
      prep.execute();
      prep.setObject(1, (byte) 3, Types.TINYINT);
      prep.execute();
      prep.setObject(1, "4", Types.TINYINT);
      prep.execute();
      prep.setObject(1, Byte.valueOf("4"), Types.TINYINT);
      prep.execute();
      prep.setObject(1, null, Types.TINYINT);
      prep.execute();
      prep.setObject(1, "true", Types.BOOLEAN);
      prep.execute();
      prep.setObject(1, "false", Types.BOOLEAN);
      prep.execute();
    }
    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM TinyIntCodec2");
    assertTrue(rs.next());
    assertEquals(1, rs.getByte(2));
    rs.updateByte("t1", (byte) 10);
    rs.updateRow();
    assertEquals((byte) 10, rs.getByte(2));

    assertTrue(rs.next());
    assertTrue(rs.getBoolean(2));
    rs.updateBoolean("t1", false);
    rs.updateRow();
    assertFalse(rs.getBoolean(2));

    assertTrue(rs.next());
    assertFalse(rs.getBoolean(2));
    rs.updateBoolean("t1", true);
    rs.updateRow();
    assertTrue(rs.getBoolean(2));

    assertTrue(rs.next());
    assertTrue(rs.getBoolean(2));
    rs.updateBoolean(2, false);
    rs.updateRow();
    assertFalse(rs.getBoolean(2));

    assertTrue(rs.next());
    assertFalse(rs.getBoolean(2));
    rs.updateBoolean(2, true);
    rs.updateRow();
    assertTrue(rs.getBoolean(2));

    assertTrue(rs.next());
    assertEquals(2, rs.getByte(2));
    rs.updateObject(2, null);
    rs.updateRow();
    assertEquals(0, rs.getByte(2));
    assertTrue(rs.wasNull());

    assertTrue(rs.next());
    assertEquals(0, rs.getByte(2));
    assertTrue(rs.wasNull());
    rs.updateByte(2, (byte) 15);
    rs.updateRow();
    assertEquals((byte) 15, rs.getByte(2));

    assertTrue(rs.next());
    assertEquals(3, rs.getByte(2));
    assertTrue(rs.next());
    assertEquals(4, rs.getByte(2));
    assertTrue(rs.next());
    assertEquals(4, rs.getShort(2));
    assertTrue(rs.next());
    assertEquals(0, rs.getByte(2));
    assertTrue(rs.wasNull());
    assertTrue(rs.next());
    assertTrue(rs.getBoolean(2));
    assertTrue(rs.next());
    assertFalse(rs.getBoolean(2));

    rs = stmt.executeQuery("SELECT * FROM TinyIntCodec2");
    assertTrue(rs.next());
    assertEquals((byte) 10, rs.getByte(2));

    assertTrue(rs.next());
    assertFalse(rs.getBoolean(2));

    assertTrue(rs.next());
    assertTrue(rs.getBoolean(2));

    assertTrue(rs.next());
    assertFalse(rs.getBoolean(2));

    assertTrue(rs.next());
    assertTrue(rs.getBoolean(2));

    assertTrue(rs.next());
    assertEquals(0, rs.getByte(2));
    assertTrue(rs.wasNull());

    assertTrue(rs.next());
    assertEquals((byte) 15, rs.getByte(2));

    assertTrue(rs.next());
    assertEquals(3, rs.getByte(2));
    assertTrue(rs.next());
    assertEquals(4, rs.getShort(2));
    assertTrue(rs.next());
    assertEquals(4, rs.getShort(2));
    assertTrue(rs.next());
    assertEquals(0, rs.getByte(2));
    assertTrue(rs.wasNull());
    con.commit();
  }
}
