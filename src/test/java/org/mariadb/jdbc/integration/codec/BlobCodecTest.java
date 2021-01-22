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
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS BlobCodec");
    stmt.execute("DROP TABLE IF EXISTS BlobCodec2");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE BlobCodec (t1 TINYBLOB, t2 TINYBLOB, t3 TINYBLOB, t4 TINYBLOB)");
    stmt.execute(
        "INSERT INTO BlobCodec VALUES ('0', '1', 'some🌟', null), ('2011-01-01', '2010-12-31 23:59:59.152',"
            + " '23:54:51.840010', null)");
    stmt.execute("CREATE TABLE BlobCodec2 (t1 TINYBLOB)");

    stmt.execute("FLUSH TABLES");
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from BlobCodec");
    assertTrue(rs.next());
    return rs;
  }

  private ResultSet getPrepare(Connection con) throws SQLException {
    PreparedStatement stmt =
        con.prepareStatement(
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
    getObject(getPrepare(sharedConn));
    getObject(getPrepare(sharedConnBinary));
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
    getObjectType(getPrepare(sharedConn));
    getObjectType(getPrepare(sharedConnBinary));
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
    getString(getPrepare(sharedConn));
    getString(getPrepare(sharedConnBinary));
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
    getNString(getPrepare(sharedConn));
    getNString(getPrepare(sharedConnBinary));
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
    getBoolean(getPrepare(sharedConn));
    getBoolean(getPrepare(sharedConnBinary));
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
    getByte(getPrepare(sharedConn));
    getByte(getPrepare(sharedConnBinary));
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
    getShort(getPrepare(sharedConn));
    getShort(getPrepare(sharedConnBinary));
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
    getInt(getPrepare(sharedConn));
    getInt(getPrepare(sharedConnBinary));
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
    getLong(getPrepare(sharedConn));
    getLong(getPrepare(sharedConnBinary));
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
    getFloat(getPrepare(sharedConn));
    getFloat(getPrepare(sharedConnBinary));
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
    getDouble(getPrepare(sharedConn));
    getDouble(getPrepare(sharedConnBinary));
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
    getBigDecimal(getPrepare(sharedConn));
    getBigDecimal(getPrepare(sharedConnBinary));
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
    getDate(getPrepare(sharedConn));
    getDate(getPrepare(sharedConnBinary));
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
    getTime(getPrepare(sharedConn));
    getTime(getPrepare(sharedConnBinary));
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
    getTimestamp(getPrepare(sharedConn));
    getTimestamp(getPrepare(sharedConnBinary));
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
    getAsciiStream(getPrepare(sharedConn));
    getAsciiStream(getPrepare(sharedConnBinary));
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
    getUnicodeStream(getPrepare(sharedConn));
    getUnicodeStream(getPrepare(sharedConnBinary));
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
    getBinaryStream(getPrepare(sharedConn));
    getBinaryStream(getPrepare(sharedConnBinary));
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
    getBytes(getPrepare(sharedConn));
    getBytes(getPrepare(sharedConnBinary));
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
    getCharacterStream(getPrepare(sharedConn));
    getCharacterStream(getPrepare(sharedConnBinary));
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
    getNCharacterStream(getPrepare(sharedConn));
    getNCharacterStream(getPrepare(sharedConnBinary));
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
    getBlob(getPrepare(sharedConn));
    getBlob(getPrepare(sharedConnBinary));
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
    getClob(getPrepare(sharedConn));
    getClob(getPrepare(sharedConnBinary));
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
    getNClob(getPrepare(sharedConn));
    getNClob(getPrepare(sharedConnBinary));
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

  @Test
  public void sendParam() throws SQLException {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE BlobCodec2");
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO BlobCodec2 VALUES (?)")) {
      prep.setBlob(1, new MariaDbBlob("e🌟1".getBytes(StandardCharsets.UTF_8)));
      prep.execute();
      prep.setBlob(1, (Blob) null);
      prep.execute();
      prep.setObject(1, new MariaDbBlob("e🌟2".getBytes(StandardCharsets.UTF_8)));
      prep.execute();
      prep.setObject(1, null);
      prep.execute();
      prep.setObject(1, new MariaDbBlob("e🌟3".getBytes(StandardCharsets.UTF_8)), Types.BLOB);
      prep.execute();
      prep.setObject(1, null, Types.BLOB);
      prep.execute();
    }

    ResultSet rs = stmt.executeQuery("SELECT * FROM BlobCodec2");
    assertTrue(rs.next());
    assertArrayEquals(
        "e🌟1".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(1).getBytes(1, (int) rs.getBlob(1).length()));
    assertTrue(rs.next());
    assertNull(rs.getBlob(1));
    assertTrue(rs.next());
    assertArrayEquals(
        "e🌟2".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(1).getBytes(1, (int) rs.getBlob(1).length()));
    assertTrue(rs.next());
    assertNull(rs.getBlob(1));
    assertTrue(rs.next());
    assertArrayEquals(
        "e🌟3".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(1).getBytes(1, (int) rs.getBlob(1).length()));
    assertTrue(rs.next());
    assertNull(rs.getBlob(1));
  }
}
