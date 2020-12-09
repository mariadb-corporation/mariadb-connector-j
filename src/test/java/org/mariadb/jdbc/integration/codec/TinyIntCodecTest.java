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

public class TinyIntCodecTest extends CommonCodecTest {
  @AfterAll
  public static void after2() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE TinyIntCodec");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS TinyIntCodec");
    stmt.execute("DROP TABLE IF EXISTS TinyIntCodecUnsigned");
    stmt.execute("CREATE TABLE TinyIntCodec (t1 TINYINT, t2 TINYINT, t3 TINYINT, t4 TINYINT)");
    stmt.execute(
        "CREATE TABLE TinyIntCodecUnsigned (t1 TINYINT UNSIGNED, t2 TINYINT UNSIGNED, t3 TINYINT UNSIGNED, t4 TINYINT "
            + "UNSIGNED)");
    stmt.execute("INSERT INTO TinyIntCodec VALUES (0, 1, -1, null)");
    stmt.execute("INSERT INTO TinyIntCodecUnsigned VALUES (0, 1, 255, null)");
  }

  private ResultSet getSigned() throws SQLException {
    return get("TinyIntCodec");
  }

  private ResultSet getUnsigned() throws SQLException {
    return get("TinyIntCodecUnsigned");
  }

  private ResultSet getPrepareSigned() throws SQLException {
    return getPrepare("TinyIntCodec");
  }

  private ResultSet getPrepareUnsigned() throws SQLException {
    return getPrepare("TinyIntCodecUnsigned");
  }

  private ResultSet get(String table) throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from " + table);
    assertTrue(rs.next());
    return rs;
  }

  private ResultSet getPrepare(String table) throws SQLException {
    PreparedStatement stmt =
        sharedConn.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from "
                + table
                + " WHERE 1 > ?");
    stmt.closeOnCompletion();
    stmt.setInt(1, 0);
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    return rs;
  }

  @Test
  public void getObject() throws Exception {
    getObject(getSigned());
  }

  @Test
  public void getObjectPrepare() throws Exception {
    getObject(getPrepareSigned());
  }

  public void getObject(ResultSet rs) throws Exception {
    assertEquals((byte) 0, rs.getObject(1));
    assertFalse(rs.wasNull());
    assertEquals((byte) 1, rs.getObject(2));
    assertEquals((byte) 1, rs.getObject("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals((byte) -1, rs.getObject(3));
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
    getObjectUnsigned(getPrepareUnsigned());
  }

  public void getObjectUnsigned(ResultSet rs) throws Exception {
    assertEquals((short) 0, rs.getObject(1));
    assertFalse(rs.wasNull());
    assertEquals((short) 1, rs.getObject(2));
    assertEquals((short) 1, rs.getObject("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals((short) 255, rs.getObject(3));
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
    getObjectType(getPrepareSigned());
  }

  public void getObjectType(ResultSet rs) throws Exception {
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
  public void getObjectTypeBoolean() throws Exception {
    getObjectTypeBoolean(getSigned());
  }

  @Test
  public void getObjectTypeBooleanPrepare() throws Exception {
    getObjectTypeBoolean(getPrepareSigned());
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
    getString(getPrepareSigned());
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
  public void getNString() throws Exception {
    getNString(getSigned());
  }

  @Test
  public void getNStringPrepare() throws Exception {
    getNString(getPrepareSigned());
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
    getBoolean(getPrepareSigned());
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
    getByte(getPrepareSigned());
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
    getShort(getPrepareSigned());
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
    getInt(getPrepareSigned());
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
    getIntUnsigned(getPrepareUnsigned());
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
    getLong(getPrepareSigned());
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
    getLongUnsigned(getPrepareUnsigned());
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
    getFloat(getPrepareSigned());
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
    getFloatUnsigned(getPrepareUnsigned());
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
    getDouble(getPrepareSigned());
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
    getDoubleUnsigned(getPrepareUnsigned());
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
    getBigDecimal(getPrepareSigned());
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
    getBigDecimalUnsigned(getPrepareUnsigned());
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
  public void getDate() throws SQLException {
    getDate(getSigned());
  }

  @Test
  public void getDatePrepare() throws SQLException {
    getDate(getPrepareSigned());
  }

  public void getDate(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class, () -> rs.getDate(1), "Data type TINYINT cannot be decoded as Date");
    assertThrowsContains(
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
    getTime(getPrepareSigned());
  }

  public void getTime(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class, () -> rs.getTime(1), "Data type TINYINT cannot be decoded as Time");
    assertThrowsContains(
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
    getTimestamp(getPrepareSigned());
  }

  public void getTimestamp(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp(1),
        "Data type TINYINT cannot be decoded as Timestamp");
    assertThrowsContains(
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
    getAsciiStream(getPrepareSigned());
  }

  public void getAsciiStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getAsciiStream(1),
        "Data type TINYINT cannot be decoded as Stream");
    assertThrowsContains(
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
    getUnicodeStream(getPrepareSigned());
  }

  @SuppressWarnings("deprecation")
  public void getUnicodeStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getUnicodeStream(1),
        "Data type TINYINT cannot be decoded as Stream");
    assertThrowsContains(
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
    getBinaryStream(getPrepareSigned());
  }

  public void getBinaryStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBinaryStream(1),
        "Data type TINYINT cannot be decoded as Stream");
    assertThrowsContains(
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
    getBytes(getPrepareSigned());
  }

  public void getBytes(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBytes(1),
        "Data type TINYINT cannot be decoded as byte[]");
    assertThrowsContains(
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
    getCharacterStream(getPrepareSigned());
  }

  public void getCharacterStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type TINYINT cannot be decoded as Reader");
    assertThrowsContains(
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
    getNCharacterStream(getPrepareSigned());
  }

  public void getNCharacterStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream(1),
        "Data type TINYINT cannot be decoded as Reader");
    assertThrowsContains(
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
    getRef(getPrepareSigned());
  }

  public void getRef(ResultSet rs) throws SQLException {
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
  public void getBlobPrepare() throws SQLException {
    getBlob(getPrepareSigned());
  }

  public void getBlob(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type TINYINT cannot be decoded as Reader");
    assertThrowsContains(
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
    getClob(getPrepareSigned());
  }

  public void getClob(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class, () -> rs.getClob(1), "Data type TINYINT cannot be decoded as Clob");
    assertThrowsContains(
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
    getNClob(getPrepareSigned());
  }

  public void getNClob(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNClob(1),
        "Data type TINYINT cannot be decoded as Clob");
    assertThrowsContains(
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
    getArray(getPrepareSigned());
  }

  public void getArray(ResultSet rs) throws SQLException {
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
  public void getURLPrepare() throws SQLException {
    getURL(getPrepareSigned());
  }

  public void getURL(ResultSet rs) throws SQLException {
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
  public void getSQLXMLPrepare() throws SQLException {
    getSQLXML(getPrepareSigned());
  }

  public void getSQLXML(ResultSet rs) throws SQLException {
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
    assertEquals("TINYINT", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.lang.Byte", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.TINYINT, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(4, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(4, meta.getColumnDisplaySize(1));
  }
}
