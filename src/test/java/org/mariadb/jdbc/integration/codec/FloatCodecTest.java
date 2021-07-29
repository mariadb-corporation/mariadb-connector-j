// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.*;
import java.time.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;

public class FloatCodecTest extends CommonCodecTest {
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS FloatCodec");
    stmt.execute("DROP TABLE IF EXISTS FloatCodec2");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE FloatCodec (t1 FLOAT, t2 FLOAT, t3 FLOAT, t4 FLOAT)");
    stmt.execute("INSERT INTO FloatCodec VALUES (0, 105.21, -1.6, null)");
    stmt.execute("CREATE TABLE FloatCodec2 (id int not null primary key auto_increment, t1 FLOAT)");
    stmt.execute("FLUSH TABLES");
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from FloatCodec");
    assertTrue(rs.next());
    return rs;
  }

  private ResultSet getPrepare(Connection con) throws SQLException {
    PreparedStatement stmt =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from FloatCodec"
                + " WHERE 1 > ?");
    stmt.closeOnCompletion();
    stmt.setInt(1, 0);
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
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
    assertEquals(0f, (float) rs.getObject(1), 0.00001f);
    assertFalse(rs.wasNull());
    assertEquals(105.21f, (float) rs.getObject(2), 0.00001f);
    assertEquals(105.21f, (float) rs.getObject("t2alias"), 0.00001f);
    assertFalse(rs.wasNull());
    assertEquals(-1.6f, (float) rs.getObject(3), 0.00001f);
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
  public void getString() throws SQLException {
    getString(get());
  }

  @Test
  public void getStringPrepare() throws SQLException {
    getString(getPrepare(sharedConn));
    getString(getPrepare(sharedConnBinary));
  }

  public void getString(ResultSet rs) throws SQLException {
    String s = rs.getString(1);
    assertTrue("0".equals(s) || "0.0".equals(s));
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
  public void getNString() throws SQLException {
    getNString(get());
  }

  @Test
  public void getNStringPrepare() throws SQLException {
    getNString(getPrepare(sharedConn));
    getNString(getPrepare(sharedConnBinary));
  }

  public void getNString(ResultSet rs) throws SQLException {
    String s = rs.getNString(1);
    assertTrue("0".equals(s) || "0.0".equals(s));
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
    assertEquals(105.21D, rs.getDouble(2), 0.01D);
    assertEquals(105.21D, rs.getDouble("t2alias"), 0.01D);
    assertFalse(rs.wasNull());
    assertEquals(-1.6D, rs.getDouble(3), 0.01D);
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

  public void getBigDecimal(ResultSet rs) throws SQLException {
    BigDecimal b = rs.getBigDecimal(1);
    assertTrue(BigDecimal.ZERO.equals(b) || new BigDecimal("0.0").equals(b));
    assertFalse(rs.wasNull());
    assertEquals(new BigDecimal("105.21"), rs.getBigDecimal(2).setScale(2, RoundingMode.HALF_DOWN));
    assertEquals(
        new BigDecimal("105.21"), rs.getBigDecimal("t2alias").setScale(2, RoundingMode.HALF_DOWN));
    assertFalse(rs.wasNull());
    assertEquals(new BigDecimal("-1.6"), rs.getBigDecimal(3).setScale(1, RoundingMode.HALF_DOWN));
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
    assertThrowsContains(
        SQLDataException.class, () -> rs.getDate(1), "Data type FLOAT cannot be decoded as Date");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getDate("t1alias"),
        "Data type FLOAT cannot be decoded as Date");
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
    assertThrowsContains(
        SQLDataException.class, () -> rs.getTime(1), "Data type FLOAT cannot be decoded as Time");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTime("t1alias"),
        "Data type FLOAT cannot be decoded as Time");
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
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp(1),
        "Data type FLOAT cannot be decoded as Timestamp");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp("t1alias"),
        "Data type FLOAT cannot be decoded as Timestamp");
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
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getAsciiStream(1),
        "Data type FLOAT cannot be decoded as Stream");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getAsciiStream("t1alias"),
        "Data type FLOAT cannot be decoded as Stream");
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
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getUnicodeStream(1),
        "Data type FLOAT cannot be decoded as Stream");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getUnicodeStream("t1alias"),
        "Data type FLOAT cannot be decoded as Stream");
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
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBinaryStream(1),
        "Data type FLOAT cannot be decoded as Stream");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBinaryStream("t1alias"),
        "Data type FLOAT cannot be decoded as Stream");
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
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBytes(1),
        "Data type FLOAT cannot be decoded as byte[]");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBytes("t1alias"),
        "Data type FLOAT cannot be decoded as byte[]");
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
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type FLOAT cannot be decoded as Reader");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream("t1alias"),
        "Data type FLOAT cannot be decoded as Reader");
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
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream(1),
        "Data type FLOAT cannot be decoded as Reader");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream("t2alias"),
        "Data type FLOAT cannot be decoded as Reader");
  }

  @Test
  public void getRef() throws SQLException {
    ResultSet rs = get();
    assertThrowsContains(
        SQLException.class, () -> rs.getRef(1), "Method ResultSet.getRef not supported");
    assertThrowsContains(
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
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type FLOAT cannot be decoded as Reader");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream("t1alias"),
        "Data type FLOAT cannot be decoded as Reader");
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
    assertThrowsContains(
        SQLDataException.class, () -> rs.getClob(1), "Data type FLOAT cannot be decoded as Clob");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getClob("t1alias"),
        "Data type FLOAT cannot be decoded as Clob");
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
    assertThrowsContains(
        SQLDataException.class, () -> rs.getNClob(1), "Data type FLOAT cannot be decoded as Clob");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNClob("t1alias"),
        "Data type FLOAT cannot be decoded as Clob");
  }

  @Test
  public void getArray() throws SQLException {
    ResultSet rs = get();
    assertThrowsContains(
        SQLException.class, () -> rs.getArray(1), "Method ResultSet.getArray not supported");
    assertThrowsContains(
        SQLException.class,
        () -> rs.getArray("t1alias"),
        "Method ResultSet.getArray not supported");
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
    assertThrowsContains(SQLSyntaxErrorException.class, () -> rs.getURL(1), "Could not parse");
    assertThrowsContains(
        SQLSyntaxErrorException.class, () -> rs.getURL("t1alias"), "Could not parse");
  }

  @Test
  public void getSQLXML() throws SQLException {
    ResultSet rs = get();
    assertThrowsContains(
        SQLException.class, () -> rs.getSQLXML(1), "Method ResultSet.getSQLXML not supported");
    assertThrowsContains(
        SQLException.class,
        () -> rs.getSQLXML("t1alias"),
        "Method ResultSet.getSQLXML not supported");
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = get();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("FLOAT", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.lang.Float", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.REAL, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(12, meta.getPrecision(1));
    assertEquals(31, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(12, meta.getColumnDisplaySize(1));
  }

  @Test
  public void sendParam() throws SQLException {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE FloatCodec2");
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO FloatCodec2(t1) VALUES (?)")) {
      prep.setFloat(1, 1.1F);
      prep.execute();
      prep.setObject(1, 2.1F);
      prep.execute();
      prep.setObject(1, null);
      prep.execute();
      prep.setObject(1, 3.1F, Types.DECIMAL);
      prep.execute();
      prep.setObject(1, null, Types.DECIMAL);
      prep.execute();
    }
    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM FloatCodec2");
    assertTrue(rs.next());
    assertEquals(1.1F, rs.getFloat(2));
    rs.updateFloat(2, 4.4f);
    rs.updateRow();
    assertEquals(4.4F, rs.getFloat(2));

    assertTrue(rs.next());
    assertEquals(2.1F, rs.getFloat(2));
    rs.updateFloat("t1", 5.4f);
    rs.updateRow();
    assertEquals(5.4F, rs.getFloat(2));

    assertTrue(rs.next());
    assertEquals(0F, rs.getFloat(2));
    assertTrue(rs.wasNull());
    rs.updateObject(2, 5.1F, Types.DECIMAL);
    rs.updateRow();
    assertEquals(5.1F, rs.getFloat(2));
    assertFalse(rs.wasNull());

    assertTrue(rs.next());
    assertEquals(3.1F, rs.getFloat(2));
    rs.updateObject("t1", null, Types.DECIMAL);
    rs.updateRow();
    assertEquals(0F, rs.getFloat(2));
    assertTrue(rs.wasNull());

    assertTrue(rs.next());
    assertEquals(0F, rs.getFloat(2));
    assertTrue(rs.wasNull());

    rs = stmt.executeQuery("SELECT * FROM FloatCodec2");
    assertTrue(rs.next());
    assertEquals(4.4F, rs.getFloat(2));

    assertTrue(rs.next());
    assertEquals(5.4F, rs.getFloat(2));

    assertTrue(rs.next());
    assertEquals(5.1F, rs.getFloat(2));
    assertFalse(rs.wasNull());

    assertTrue(rs.next());
    assertEquals(0F, rs.getFloat(2));
    assertTrue(rs.wasNull());

    assertTrue(rs.next());
    assertEquals(0F, rs.getFloat(2));
    assertTrue(rs.wasNull());
  }
}
