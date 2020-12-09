package org.mariadb.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.Statement;

public class BlobCodecTest extends CommonCodecTest {
  @AfterAll
  public static void after2() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE BlobCodec");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS BlobCodec");
    stmt.execute(
        "CREATE TABLE BlobCodec (t1 TINYBLOB, t2 TINYBLOB, t3 TINYBLOB, t4 TINYBLOB) CHARACTER "
            + "SET utf8mb4 COLLATE utf8mb4_unicode_ci");
    stmt.execute(
        "INSERT INTO BlobCodec VALUES ('0', '1', 'some🌟', null), ('2011-01-01', '2010-12-31 23:59:59.152',"
            + " '23:54:51.840010', null)");
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from BlobCodec");
    assertTrue(rs.next());
    return rs;
  }

  private ResultSet getPrepare() throws SQLException {
    PreparedStatement stmt =
        sharedConn.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from BlobCodec"
                + " WHERE 1 > ?");
    stmt.closeOnCompletion();
    stmt.setInt(1, 0);
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    return rs;
  }

  @Test
  public void getObject() throws Exception {
    getObject(get());
  }

  @Test
  public void getObjectPrepare() throws Exception {
    getObject(getPrepare());
  }

  public void getObject(ResultSet rs) throws Exception {
    assertStreamEquals(new MariaDbBlob("0".getBytes()), (Blob) rs.getObject(1));
    assertFalse(rs.wasNull());
    assertStreamEquals(new MariaDbBlob("1".getBytes()), (Blob) rs.getObject(2));
    assertStreamEquals(new MariaDbBlob("1".getBytes()), (Blob) rs.getObject("t2alias"));
    assertFalse(rs.wasNull());
    assertStreamEquals(
        new MariaDbBlob("some🌟".getBytes(StandardCharsets.UTF_8)), (Blob) rs.getObject(3));
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
    getObjectType(getPrepare());
  }

  public void getObjectType(ResultSet rs) throws Exception {
    testErrObject(rs, Integer.class);
    testErrObject(rs, String.class);
    testErrObject(rs, Long.class);
    testErrObject(rs, Short.class);
    testErrObject(rs, BigDecimal.class);
    testErrObject(rs, BigInteger.class);
    testErrObject(rs, Double.class);
    testErrObject(rs, Float.class);
    testObject(rs, Byte.class, Byte.valueOf((byte) '0'));
    testArrObject(rs, byte[].class, new byte[] {(byte) '0'});
    testErrObject(rs, Boolean.class);
    testErrObject(rs, Clob.class);
    testErrObject(rs, NClob.class);
    testObject(rs, InputStream.class, new MariaDbBlob("0".getBytes()).getBinaryStream());
    testErrObject(rs, Reader.class);
    rs.next();
    testErrObject(rs, LocalDate.class);
    testErrObject(rs, LocalDateTime.class);
    testErrObject(rs, Time.class);
    testErrObject(rs, Date.class);
    testErrObject(rs, Timestamp.class);
    testErrObject(rs, ZonedDateTime.class);
    testErrObject(rs, OffsetDateTime.class);
    testErrObject(rs, OffsetTime.class);
    testErrObject(rs, java.util.Date.class);
  }

  @Test
  public void getString() throws SQLException {
    getString(get());
  }

  @Test
  public void getStringPrepare() throws SQLException {
    getString(getPrepare());
  }

  public void getString(ResultSet rs) throws SQLException {
    assertEquals("0", rs.getString(1));
    assertFalse(rs.wasNull());
    assertEquals("1", rs.getString(2));
    assertEquals("1", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("some🌟", rs.getString(3));
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
    getNString(getPrepare());
  }

  public void getNString(ResultSet rs) throws SQLException {
    assertEquals("0", rs.getNString(1));
    assertFalse(rs.wasNull());
    assertEquals("1", rs.getNString(2));
    assertEquals("1", rs.getNString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("some🌟", rs.getNString(3));
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
    getBoolean(getPrepare());
  }

  public void getBoolean(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBoolean(1),
        "Data type BLOB cannot be decoded as Boolean");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getByte() throws SQLException {
    getByte(get());
  }

  @Test
  public void getBytePrepare() throws SQLException {
    getByte(getPrepare());
  }

  public void getByte(ResultSet rs) throws SQLException {
    assertEquals((byte) 48, rs.getByte(1));
    assertFalse(rs.wasNull());
    assertEquals((byte) 49, rs.getByte(2));
    assertEquals((byte) 49, rs.getByte("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals((byte) 115, rs.getByte(3));
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
    getShort(getPrepare());
  }

  public void getShort(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class, () -> rs.getShort(1), "Data type BLOB cannot be decoded as Short");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getInt() throws SQLException {
    getInt(get());
  }

  @Test
  public void getIntPrepare() throws SQLException {
    getInt(getPrepare());
  }

  public void getInt(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class, () -> rs.getInt(1), "Data type BLOB cannot be decoded as Integer");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getLong() throws SQLException {
    getLong(get());
  }

  @Test
  public void getLongPrepare() throws SQLException {
    getLong(getPrepare());
  }

  public void getLong(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class, () -> rs.getLong(1), "Data type BLOB cannot be decoded as Long");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getFloat() throws SQLException {
    getFloat(get());
  }

  @Test
  public void getFloatPrepare() throws SQLException {
    getFloat(getPrepare());
  }

  public void getFloat(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class, () -> rs.getFloat(1), "Data type BLOB cannot be decoded as Float");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getDouble() throws SQLException {
    getDouble(get());
  }

  @Test
  public void getDoublePrepare() throws SQLException {
    getDouble(getPrepare());
  }

  public void getDouble(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getDouble(1),
        "Data type BLOB cannot be decoded as Double");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getBigDecimal() throws SQLException {
    getBigDecimal(get());
  }

  @Test
  public void getBigDecimalPrepare() throws SQLException {
    getBigDecimal(getPrepare());
  }

  public void getBigDecimal(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBigDecimal(1),
        "Data type BLOB cannot be decoded as BigDecimal");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getDate() throws SQLException {
    getDate(get());
  }

  @Test
  public void getDatePrepare() throws SQLException {
    getDate(getPrepare());
  }

  public void getDate(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class, () -> rs.getDate(1), "Data type BLOB cannot be decoded as Date");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getTime() throws SQLException {
    getTime(get());
  }

  @Test
  public void getTimePrepare() throws SQLException {
    getTime(getPrepare());
  }

  public void getTime(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class, () -> rs.getTime(1), "Data type BLOB cannot be decoded as Time");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getTimestamp() throws SQLException {
    getTimestamp(get());
  }

  @Test
  public void getTimestampPrepare() throws SQLException {
    getTimestamp(getPrepare());
  }

  public void getTimestamp(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp(1),
        "Data type BLOB cannot be decoded as Timestamp");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getAsciiStream() throws Exception {
    getAsciiStream(get());
  }

  @Test
  public void getAsciiStreamPrepare() throws Exception {
    getAsciiStream(getPrepare());
  }

  public void getAsciiStream(ResultSet rs) throws Exception {
    assertStreamEquals(new ByteArrayInputStream("0".getBytes()), rs.getAsciiStream(1));
    assertFalse(rs.wasNull());
    assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getAsciiStream(2));
    assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getAsciiStream("t2alias"));
    assertFalse(rs.wasNull());
    assertStreamEquals(
        new ByteArrayInputStream("some🌟".getBytes(StandardCharsets.UTF_8)), rs.getAsciiStream(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getAsciiStream(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getUnicodeStream() throws Exception {
    getUnicodeStream(get());
  }

  @Test
  public void getUnicodeStreamPrepare() throws Exception {
    getUnicodeStream(getPrepare());
  }

  @SuppressWarnings("deprecation")
  public void getUnicodeStream(ResultSet rs) throws Exception {
    assertStreamEquals(new ByteArrayInputStream("0".getBytes()), rs.getUnicodeStream(1));
    assertFalse(rs.wasNull());
    assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getUnicodeStream(2));
    assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getUnicodeStream("t2alias"));
    assertFalse(rs.wasNull());
    assertStreamEquals(
        new ByteArrayInputStream("some🌟".getBytes(StandardCharsets.UTF_8)),
        rs.getUnicodeStream(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getUnicodeStream(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBinaryStream() throws Exception {
    getBinaryStream(get());
  }

  @Test
  public void getBinaryStreamPrepare() throws Exception {
    getBinaryStream(getPrepare());
  }

  public void getBinaryStream(ResultSet rs) throws Exception {
    assertStreamEquals(new ByteArrayInputStream("0".getBytes()), rs.getBinaryStream(1));
    assertFalse(rs.wasNull());
    assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getBinaryStream(2));
    assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getBinaryStream("t2alias"));
    assertFalse(rs.wasNull());
    assertStreamEquals(
        new ByteArrayInputStream("some🌟".getBytes(StandardCharsets.UTF_8)), rs.getBinaryStream(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBinaryStream(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getBytes() throws Exception {
    getBytes(get());
  }

  @Test
  public void getBytesPrepare() throws Exception {
    getBytes(getPrepare());
  }

  public void getBytes(ResultSet rs) throws Exception {
    assertArrayEquals("0".getBytes(), rs.getBytes(1));
    assertFalse(rs.wasNull());
    assertArrayEquals("1".getBytes(), rs.getBytes(2));
    assertArrayEquals("1".getBytes(), rs.getBytes("t2alias"));
    assertFalse(rs.wasNull());
    assertArrayEquals("some🌟".getBytes(StandardCharsets.UTF_8), rs.getBytes(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBytes(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getCharacterStream() throws Exception {
    getCharacterStream(get());
  }

  @Test
  public void getCharacterStreamPrepare() throws Exception {
    getCharacterStream(getPrepare());
  }

  public void getCharacterStream(ResultSet rs) throws Exception {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type BLOB cannot be decoded as Reader");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getNCharacterStream() throws Exception {
    getNCharacterStream(get());
  }

  @Test
  public void getNCharacterStreamPrepare() throws Exception {
    getNCharacterStream(getPrepare());
  }

  public void getNCharacterStream(ResultSet rs) throws Exception {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream(1),
        "Data type BLOB cannot be decoded as Reader");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getBlob() throws Exception {
    getBlob(get());
  }

  @Test
  public void getBlobPrepare() throws Exception {
    getBlob(getPrepare());
  }

  public void getBlob(ResultSet rs) throws Exception {
    assertStreamEquals(new MariaDbBlob("0".getBytes()), rs.getBlob(1));
    assertFalse(rs.wasNull());
    assertStreamEquals(new MariaDbBlob("1".getBytes()), rs.getBlob(2));
    assertStreamEquals(new MariaDbBlob("1".getBytes()), rs.getBlob("t2alias"));
    assertFalse(rs.wasNull());
    assertStreamEquals(new MariaDbBlob("some🌟".getBytes(StandardCharsets.UTF_8)), rs.getBlob(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBlob(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getClob() throws Exception {
    getClob(get());
  }

  @Test
  public void getClobPrepare() throws Exception {
    getClob(getPrepare());
  }

  public void getClob(ResultSet rs) throws Exception {
    assertThrowsContains(
        SQLDataException.class, () -> rs.getClob(1), "Data type BLOB cannot be decoded as Clob");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getNClob() throws Exception {
    getNClob(get());
  }

  @Test
  public void getNClobPrepare() throws Exception {
    getNClob(getPrepare());
  }

  public void getNClob(ResultSet rs) throws Exception {
    assertThrowsContains(
        SQLDataException.class, () -> rs.getNClob(1), "Data type BLOB cannot be decoded as Clob");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = get();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("BLOB", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.sql.Blob", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.VARBINARY, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(255, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(255, meta.getColumnDisplaySize(1));
  }
}
