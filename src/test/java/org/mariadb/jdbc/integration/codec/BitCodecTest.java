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
import java.util.BitSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.integration.Common;

public class BitCodecTest extends CommonCodecTest {
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS BitCodec");
    stmt.execute("DROP TABLE IF EXISTS BitCodec2");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE BitCodec (t1 BIT(1), t2 BIT(4), t3 BIT(16), t4 BIT(24))");
    stmt.execute("INSERT INTO BitCodec VALUES (b'0000', b'0001', b'0000111100000100', null)");
    stmt.execute("CREATE TABLE BitCodec2 (id int not null primary key auto_increment, t1 BIT(16))");
    stmt.execute("FLUSH TABLES");
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from BitCodec");
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private ResultSet getPrepare(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    PreparedStatement preparedStatement =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from BitCodec"
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
    assertEquals(false, rs.getObject(1));
    assertFalse(rs.wasNull());
    assertArrayEquals(new byte[] {(byte) 1}, (byte[]) rs.getObject(2));
    assertEquals((byte) 1, (byte) rs.getObject(2, Byte.class));
    assertArrayEquals(new byte[] {(byte) 1}, (byte[]) rs.getObject("t2alias"));
    assertFalse(rs.wasNull());
    assertArrayEquals(new byte[] {(byte) 15, (byte) 4}, (byte[]) rs.getObject(3));
    assertEquals((byte) 15, (byte) rs.getObject(3, Byte.class));
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
    testObject(rs, Byte.class, Byte.valueOf("0"));
    testObject(rs, String.class, "b''");
    testObject(rs, Long.class, 0L);
    testObject(rs, Short.class, (short) 0);
    testObject(rs, BitSet.class, BitSet.valueOf(new byte[] {(byte) 0}));
    testObject(rs, BigDecimal.class, BigDecimal.valueOf(0));
    testObject(rs, BigInteger.class, BigInteger.valueOf(0));
    testObject(rs, Boolean.class, Boolean.FALSE);

    testErrObject(rs, Double.class);
    testErrObject(rs, Float.class);
    testObject(rs, Byte.class, (byte) 0);
    testObject(rs, byte[].class, new byte[] {0});
    testErrObject(rs, Date.class);
    testErrObject(rs, Time.class);
    testErrObject(rs, Timestamp.class);
    testErrObject(rs, java.util.Date.class);
    testErrObject(rs, LocalDate.class);
    testErrObject(rs, ZonedDateTime.class);
    testErrObject(rs, OffsetDateTime.class);
    testErrObject(rs, LocalDateTime.class);
    testErrObject(rs, OffsetTime.class);
    testErrObject(rs, Clob.class);
    testErrObject(rs, NClob.class);
    testErrObject(rs, InputStream.class);
    testErrObject(rs, Reader.class);
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
    assertEquals("b''", rs.getString(1));
    assertFalse(rs.wasNull());
    assertEquals("b'1'", rs.getString(2));
    assertEquals("b'1'", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("b'111100000100'", rs.getString(3));
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
    assertEquals("b''", rs.getNString(1));
    assertFalse(rs.wasNull());
    assertEquals("b'1'", rs.getNString(2));
    assertEquals("b'1'", rs.getNString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("b'111100000100'", rs.getNString(3));
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
    assertEquals((byte) 1, rs.getByte(2));
    assertEquals((byte) 1, rs.getByte("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals((byte) 15, rs.getByte(3));
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
    assertEquals(1, rs.getShort(2));
    assertEquals(1, rs.getShort("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(3844, rs.getShort(3));
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getShort(4));
    assertTrue(rs.wasNull());
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
    assertEquals(1, rs.getInt(2));
    assertEquals(1, rs.getInt("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(3844, rs.getInt(3));
    assertFalse(rs.wasNull());
    assertEquals(0, rs.getInt(4));
    assertTrue(rs.wasNull());
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
    assertEquals(1L, rs.getLong(2));
    assertEquals(1L, rs.getLong("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(3844L, rs.getLong(3));
    assertFalse(rs.wasNull());
    assertEquals(0L, rs.getLong(4));
    assertTrue(rs.wasNull());
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

  public void getFloat(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getFloat(1), "Data type BIT cannot be decoded as Float");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getFloat("t1alias"),
        "Data type BIT cannot be decoded as Float");
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

  public void getDouble(ResultSet rs) {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getDouble(1), "Data type BIT cannot be decoded as Double");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getDouble("t1alias"),
        "Data type BIT cannot be decoded as Double");
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
    assertEquals(BigDecimal.ZERO, rs.getBigDecimal(1));
    assertFalse(rs.wasNull());
    assertEquals(BigDecimal.ONE, rs.getBigDecimal(2));
    assertEquals(BigDecimal.ONE, rs.getBigDecimal("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(BigDecimal.valueOf(3844), rs.getBigDecimal(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBigDecimal(4));
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
        SQLDataException.class, () -> rs.getDate(1), "Data type BIT cannot be decoded as Date");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getDate("t1alias"),
        "Data type BIT cannot be decoded as Date");
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
        SQLDataException.class, () -> rs.getTime(1), "Data type BIT cannot be decoded as Time");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTime("t1alias"),
        "Data type BIT cannot be decoded as Time");
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
        "Data type BIT cannot be decoded as Timestamp");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp("t1alias"),
        "Data type BIT cannot be decoded as Timestamp");
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
        "Type class java.time.OffsetDateTime not supported type for BIT type");
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
        "Data type BIT cannot be decoded as Stream");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getAsciiStream("t1alias"),
        "Data type BIT cannot be decoded as Stream");
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
        "Data type BIT cannot be decoded as Stream");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getUnicodeStream("t1alias"),
        "Data type BIT cannot be decoded as Stream");
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
        "Data type BIT cannot be decoded as Stream");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBinaryStream("t1alias"),
        "Data type BIT cannot be decoded as Stream");
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

  public void getBytes(ResultSet rs) throws SQLException {
    assertArrayEquals(new byte[] {(byte) 0}, rs.getBytes(1));
    assertFalse(rs.wasNull());
    assertArrayEquals(new byte[] {(byte) 1}, rs.getBytes(2));
    assertArrayEquals(new byte[] {(byte) 1}, rs.getBytes("t2alias"));
    assertFalse(rs.wasNull());
    assertArrayEquals(new byte[] {(byte) 15, (byte) 4}, rs.getBytes(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4));
    assertTrue(rs.wasNull());
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
        "Data type BIT cannot be decoded as Reader");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream("t1alias"),
        "Data type BIT cannot be decoded as Reader");
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
        "Data type BIT cannot be decoded as Reader");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream("t2alias"),
        "Data type BIT cannot be decoded as Reader");
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

  public void getBlob(ResultSet rs) throws SQLException {

    assertEquals(new MariaDbBlob(new byte[] {(byte) 0x00}), rs.getBlob(1));
    assertFalse(rs.wasNull());
    assertEquals(new MariaDbBlob(new byte[] {(byte) 0x01}), rs.getBlob(2));
    assertEquals(new MariaDbBlob(new byte[] {0x01}), rs.getBlob("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(new MariaDbBlob(new byte[] {(byte) 15, (byte) 4}), rs.getBlob(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBlob(4));
    assertTrue(rs.wasNull());
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
        SQLDataException.class, () -> rs.getClob(1), "Data type BIT cannot be decoded as Clob");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getClob("t1alias"),
        "Data type BIT cannot be decoded as Clob");
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
        SQLDataException.class, () -> rs.getNClob(1), "Data type BIT cannot be decoded as Clob");
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNClob("t1alias"),
        "Data type BIT cannot be decoded as Clob");
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
        SQLSyntaxErrorException.class, () -> rs.getURL(1), "Could not parse 'b''' as URL");
    Common.assertThrowsContains(
        SQLSyntaxErrorException.class, () -> rs.getURL("t1alias"), "Could not parse 'b''' as URL");
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = get();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("BIT", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.lang.Boolean", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.BOOLEAN, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(1, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(1, meta.getColumnDisplaySize(1));

    assertEquals("BIT", meta.getColumnTypeName(2));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(2));
    assertEquals("byte[]", meta.getColumnClassName(2));
    assertEquals("t2alias", meta.getColumnLabel(2));
    assertEquals("t2", meta.getColumnName(2));
    assertEquals(Types.BIT, meta.getColumnType(2));
    assertEquals(4, meta.getPrecision(2));
    assertEquals(0, meta.getScale(2));
    assertEquals("", meta.getSchemaName(2));
    assertEquals(4, meta.getColumnDisplaySize(2));
  }

  @Test
  public void sendParam() throws SQLException {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE BitCodec2");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO BitCodec2(t1) VALUES (?)")) {
      prep.setObject(1, BitSet.valueOf(new byte[] {0x00, 0x01}));
      prep.execute();
      prep.setObject(1, null);
      prep.execute();
      prep.setObject(1, BitSet.valueOf(new byte[] {0x00, 0x02}), Types.BINARY);
      prep.execute();
      prep.setObject(1, null, Types.BINARY);
      prep.execute();
      prep.setObject(1, 0, Types.BIT);
      prep.execute();
      prep.setObject(1, 1, Types.BIT);
      prep.execute();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM BitCodec2");
    assertTrue(rs.next());
    assertEquals("b'100000000'", rs.getString(2));
    rs.updateObject(2, BitSet.valueOf(new byte[] {0x02, 0x00}));
    rs.updateRow();
    assertEquals("b'10'", rs.getString(2));

    assertTrue(rs.next());
    assertNull(rs.getBytes(2));
    rs.updateObject(2, BitSet.valueOf(new byte[] {0x03, 0x00}), Types.BINARY);
    rs.updateRow();
    assertEquals("b'11'", rs.getString(2));

    assertTrue(rs.next());
    assertEquals("b'1000000000'", rs.getString(2));
    rs.updateObject(2, null);
    rs.updateRow();
    assertNull(rs.getBytes(2));

    assertTrue(rs.next());
    assertNull(rs.getBytes(2));
    rs.updateObject("t1", BitSet.valueOf(new byte[] {0x04, 0x00}), Types.BINARY);
    rs.updateRow();
    assertEquals("b'100'", rs.getString(2));
    assertTrue(rs.next());
    assertEquals("b''", rs.getString(2));
    assertTrue(rs.next());
    assertEquals("b'1'", rs.getString(2));

    rs = stmt.executeQuery("SELECT * FROM BitCodec2");
    assertTrue(rs.next());
    assertEquals("b'10'", rs.getString(2));
    assertTrue(rs.next());
    assertEquals("b'11'", rs.getString(2));
    assertTrue(rs.next());
    assertNull(rs.getBytes(2));
    assertTrue(rs.next());
    assertEquals("b'100'", rs.getString(2));
    con.commit();
  }
}
