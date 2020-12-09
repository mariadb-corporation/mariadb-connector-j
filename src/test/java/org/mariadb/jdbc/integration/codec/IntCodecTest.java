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

public class IntCodecTest extends CommonCodecTest {
  @AfterAll
  public static void after2() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE IntCodec");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS IntCodec");
    stmt.execute("DROP TABLE IF EXISTS IntCodecUnsigned");
    stmt.execute("CREATE TABLE IntCodec (t1 int, t2 int, t3 int, t4 int)");
    stmt.execute(
        "CREATE TABLE IntCodecUnsigned (t1 INT UNSIGNED, t2 INT UNSIGNED, t3 INT UNSIGNED, t4 INT "
            + "UNSIGNED)");
    stmt.execute("INSERT INTO IntCodec VALUES (0, 1, -1, null)");
    stmt.execute("INSERT INTO IntCodecUnsigned VALUES (0, 1, 4294967295, null)");
  }

  private ResultSet getSigned() throws SQLException {
    return get("IntCodec");
  }

  private ResultSet getUnsigned() throws SQLException {
    return get("IntCodecUnsigned");
  }

  private ResultSet get(String table) throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from " + table);
    assertTrue(rs.next());
    return rs;
  }

  private ResultSet getPreparedSigned() throws SQLException {
    return getPrepare("IntCodec");
  }

  private ResultSet getPreparedUnsigned() throws SQLException {
    return getPrepare("IntCodecUnsigned");
  }

  private ResultSet getPrepare(String table) throws SQLException {
    try (PreparedStatement stmt =
        sharedConn.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4"
                + " as t4alias from "
                + table
                + " WHERE 1 > ?")) {
      stmt.setInt(1, 0);
      ResultSet rs = stmt.executeQuery();
      assertTrue(rs.next());
      return rs;
    }
  }

  @Test
  public void getObject() throws SQLException {
    getObject(getSigned());
  }

  @Test
  public void getObjectPrepared() throws SQLException {
    getObject(getPreparedSigned());
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
    getObjectType(getPreparedSigned());
  }

  private void getObjectType(ResultSet rs) throws Exception {
    testObject(rs, Integer.class, Integer.valueOf(0));
    testObject(rs, String.class, "0");
    testObject(rs, Long.class, Long.valueOf(0));
    testObject(rs, Short.class, Short.valueOf((short) 0));
    testObject(rs, BigDecimal.class, BigDecimal.valueOf(0));
    testObject(rs, BigInteger.class, BigInteger.valueOf(0));
    testObject(rs, Double.class, Double.valueOf(0));
    testObject(rs, Float.class, Float.valueOf(0));
    testObject(rs, Byte.class, Byte.valueOf((byte) 0));
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
    getString(getPreparedSigned());
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
  public void getNString() throws SQLException {
    getNString(getSigned());
  }

  @Test
  public void getNStringPrepared() throws SQLException {
    getNString(getPreparedSigned());
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
    getBoolean(getPreparedSigned());
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
    getByte(getPreparedSigned());
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
    getShort(getPreparedSigned());
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
    getInt(getPreparedSigned());
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
    getIntUnsigned(getPreparedUnsigned());
  }

  private void getIntUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0, rs.getInt(1));
    assertFalse(rs.wasNull());
    assertEquals(1, rs.getInt(2));
    assertFalse(rs.wasNull());
    assertThrowsContains(SQLDataException.class, () -> rs.getInt(3), "integer overflow");
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
    getLong(getPreparedSigned());
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
    getLongUnsigned(getPreparedUnsigned());
  }

  private void getLongUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0L, rs.getLong(1));
    assertFalse(rs.wasNull());
    assertEquals(1L, rs.getLong(2));
    assertEquals(1L, rs.getLong("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(4294967295L, rs.getLong(3));
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
    getFloat(getPreparedSigned());
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
    getFloatUnsigned(getPreparedUnsigned());
  }

  private void getFloatUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0F, rs.getFloat(1));
    assertFalse(rs.wasNull());
    assertEquals(1F, rs.getFloat(2));
    assertEquals(1F, rs.getFloat("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(4294967295F, rs.getFloat(3));
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
    getDouble(getPreparedSigned());
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
    getDoubleUnsigned(getPreparedUnsigned());
  }

  private void getDoubleUnsigned(ResultSet rs) throws SQLException {
    assertEquals(0D, rs.getDouble(1));
    assertFalse(rs.wasNull());
    assertEquals(1D, rs.getDouble(2));
    assertEquals(1D, rs.getDouble("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(4294967295D, rs.getDouble(3));
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
    getBigDecimal(getPreparedSigned());
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
    getBigDecimalUnsigned(getPreparedUnsigned());
  }

  private void getBigDecimalUnsigned(ResultSet rs) throws SQLException {
    assertEquals(BigDecimal.ZERO, rs.getBigDecimal(1));
    assertFalse(rs.wasNull());
    assertEquals(BigDecimal.ONE, rs.getBigDecimal(2));
    assertEquals(BigDecimal.ONE, rs.getBigDecimal("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(BigDecimal.valueOf(4294967295L), rs.getBigDecimal(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBigDecimal(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getDate() throws SQLException {
    getDate(getSigned());
  }

  @Test
  public void getDatePrepared() throws SQLException {
    getDate(getPreparedSigned());
  }

  private void getDate(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class, () -> rs.getDate(1), "Data type INTEGER cannot be decoded as Date");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getDate("t1alias"),
        "Data type INTEGER cannot be decoded as Date");
  }

  @Test
  public void getTime() throws SQLException {
    getTime(getSigned());
  }

  @Test
  public void getTimePrepared() throws SQLException {
    getTime(getPreparedSigned());
  }

  private void getTime(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class, () -> rs.getTime(1), "Data type INTEGER cannot be decoded as Time");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTime("t1alias"),
        "Data type INTEGER cannot be decoded as Time");
  }

  @Test
  public void getTimestamp() throws SQLException {
    getTimestamp(getSigned());
  }

  @Test
  public void getTimestampPrepared() throws SQLException {
    getTimestamp(getPreparedSigned());
  }

  private void getTimestamp(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp(1),
        "Data type INTEGER cannot be decoded as Timestamp");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp("t1alias"),
        "Data type INTEGER cannot be decoded as Timestamp");
  }

  @Test
  public void getAsciiStream() throws SQLException {
    getAsciiStream(getSigned());
  }

  @Test
  public void getAsciiStreamPrepared() throws SQLException {
    getAsciiStream(getPreparedSigned());
  }

  private void getAsciiStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getAsciiStream(1),
        "Data type INTEGER cannot be decoded as Stream");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getAsciiStream("t1alias"),
        "Data type INTEGER cannot be decoded as Stream");
  }

  @Test
  public void getUnicodeStream() throws SQLException {
    getUnicodeStream(getSigned());
  }

  @Test
  public void getUnicodeStreamPrepared() throws SQLException {
    getUnicodeStream(getPreparedSigned());
  }

  @SuppressWarnings("deprecation")
  private void getUnicodeStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getUnicodeStream(1),
        "Data type INTEGER cannot be decoded as Stream");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getUnicodeStream("t1alias"),
        "Data type INTEGER cannot be decoded as Stream");
  }

  @Test
  public void getBinaryStream() throws SQLException {
    getBinaryStream(getSigned());
  }

  @Test
  public void getBinaryStreamPrepared() throws SQLException {
    getBinaryStream(getPreparedSigned());
  }

  private void getBinaryStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBinaryStream(1),
        "Data type INTEGER cannot be decoded as Stream");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBinaryStream("t1alias"),
        "Data type INTEGER cannot be decoded as Stream");
  }

  @Test
  public void getBytes() throws SQLException {
    getBytes(getSigned());
  }

  @Test
  public void getBytesPrepared() throws SQLException {
    getBytes(getPreparedSigned());
  }

  private void getBytes(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBytes(1),
        "Data type INTEGER cannot be decoded as byte[]");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBytes("t1alias"),
        "Data type INTEGER cannot be decoded as byte[]");
  }

  @Test
  public void getCharacterStream() throws SQLException {
    getCharacterStream(getSigned());
  }

  @Test
  public void getCharacterStreamPrepared() throws SQLException {
    getCharacterStream(getPreparedSigned());
  }

  private void getCharacterStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type INTEGER cannot be decoded as Reader");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream("t1alias"),
        "Data type INTEGER cannot be decoded as Reader");
  }

  @Test
  public void getNCharacterStream() throws SQLException {
    getNCharacterStream(getSigned());
  }

  @Test
  public void getNCharacterStreamPrepared() throws SQLException {
    getNCharacterStream(getPreparedSigned());
  }

  private void getNCharacterStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream(1),
        "Data type INTEGER cannot be decoded as Reader");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream("t2alias"),
        "Data type INTEGER cannot be decoded as Reader");
  }

  @Test
  public void getRef() throws SQLException {
    getRef(getSigned());
  }

  @Test
  public void getRefPrepared() throws SQLException {
    getRef(getPreparedSigned());
  }

  private void getRef(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getRef(1), "Method ResultSet.getRef not supported");
    assertThrowsContains(
        SQLException.class, () -> rs.getRef("t2alias"), "Method ResultSet.getRef not supported");
  }

  @Test
  public void getBlob() throws SQLException {
    getBlob(getSigned());
  }

  @Test
  public void getBlobPrepared() throws SQLException {
    getBlob(getPreparedSigned());
  }

  private void getBlob(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type INTEGER cannot be decoded as Reader");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream("t1alias"),
        "Data type INTEGER cannot be decoded as Reader");
  }

  @Test
  public void getClob() throws SQLException {
    getClob(getSigned());
  }

  @Test
  public void getClobPrepared() throws SQLException {
    getClob(getPreparedSigned());
  }

  private void getClob(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class, () -> rs.getClob(1), "Data type INTEGER cannot be decoded as Clob");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getClob("t1alias"),
        "Data type INTEGER cannot be decoded as Clob");
  }

  @Test
  public void getNClob() throws SQLException {
    getNClob(getSigned());
  }

  @Test
  public void getNClobPrepared() throws SQLException {
    getNClob(getPreparedSigned());
  }

  private void getNClob(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNClob(1),
        "Data type INTEGER cannot be decoded as Clob");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNClob("t1alias"),
        "Data type INTEGER cannot be decoded as Clob");
  }

  @Test
  public void getArray() throws SQLException {
    getArray(getSigned());
  }

  @Test
  public void getArrayPrepared() throws SQLException {
    getArray(getPreparedSigned());
  }

  private void getArray(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getArray(1), "Method ResultSet.getArray not supported");
    assertThrowsContains(
        SQLException.class,
        () -> rs.getArray("t1alias"),
        "Method ResultSet.getArray not supported");
  }

  @Test
  public void getURL() throws SQLException {
    getURL(getSigned());
  }

  @Test
  public void getURLPrepared() throws SQLException {
    getURL(getPreparedSigned());
  }

  private void getURL(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLSyntaxErrorException.class, () -> rs.getURL(1), "Could not parse '0' as URL");
    assertThrowsContains(
        SQLSyntaxErrorException.class, () -> rs.getURL("t1alias"), "Could not parse '0' as URL");
  }

  @Test
  public void getSQLXML() throws SQLException {
    getSQLXML(getSigned());
  }

  @Test
  public void getSQLXMLPrepared() throws SQLException {
    getSQLXML(getPreparedSigned());
  }

  private void getSQLXML(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getSQLXML(1), "Method ResultSet.getSQLXML not supported");
    assertThrowsContains(
        SQLException.class,
        () -> rs.getSQLXML("t1alias"),
        "Method ResultSet.getSQLXML not supported");
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = getSigned();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("INTEGER", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.lang.Integer", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.INTEGER, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(11, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(11, meta.getColumnDisplaySize(1));
  }
}
