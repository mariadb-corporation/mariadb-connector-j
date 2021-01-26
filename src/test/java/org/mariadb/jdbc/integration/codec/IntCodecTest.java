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

public class IntCodecTest extends CommonCodecTest {
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS IntCodec");
    stmt.execute("DROP TABLE IF EXISTS IntCodecUnsigned");
    stmt.execute("DROP TABLE IF EXISTS IntCodec2");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE IntCodec (t1 int, t2 int, t3 int, t4 int)");
    stmt.execute(
        "CREATE TABLE IntCodecUnsigned (t1 INT UNSIGNED, t2 INT UNSIGNED, t3 INT UNSIGNED, t4 INT "
            + "UNSIGNED)");
    stmt.execute("INSERT INTO IntCodec VALUES (0, 1, -1, null)");
    stmt.execute("INSERT INTO IntCodecUnsigned VALUES (0, 1, 4294967295, null)");
    stmt.execute("CREATE TABLE IntCodec2 (t1 int)");
    stmt.execute("FLUSH TABLES");
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

  private ResultSet getPreparedSigned(Connection con) throws SQLException {
    return getPrepare(con, "IntCodec");
  }

  private ResultSet getPreparedUnsigned(Connection con) throws SQLException {
    return getPrepare(con, "IntCodecUnsigned");
  }

  private ResultSet getPrepare(Connection con, String table) throws SQLException {
    try (PreparedStatement stmt =
        con.prepareStatement(
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
    getObject(getPreparedSigned(sharedConn));
    getObject(getPreparedSigned(sharedConnBinary));
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
    getObjectType(getPreparedSigned(sharedConn));
    getObjectType(getPreparedSigned(sharedConnBinary));
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
    getNString(getPreparedSigned(sharedConn));
    getNString(getPreparedSigned(sharedConnBinary));
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
    getBoolean(getPreparedSigned(sharedConn));
    getBoolean(getPreparedSigned(sharedConnBinary));
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
    getByte(getPreparedSigned(sharedConn));
    getByte(getPreparedSigned(sharedConnBinary));
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
  public void getUnsignedByte() throws SQLException {
    getUnsignedByte(getUnsigned());
  }

  @Test
  public void getUnsignedBytePrepared() throws SQLException {
    getUnsignedByte(getPreparedUnsigned(sharedConn));
    getUnsignedByte(getPreparedUnsigned(sharedConnBinary));
  }

  private void getUnsignedByte(ResultSet rs) throws SQLException {
    assertEquals((byte) 0, rs.getByte(1));
    assertFalse(rs.wasNull());
    assertEquals((byte) 1, rs.getByte(2));
    assertEquals((byte) 1, rs.getByte("t2alias"));
    assertFalse(rs.wasNull());
    assertThrowsContains(SQLDataException.class, () -> rs.getByte(3), "byte overflow");
    assertFalse(rs.wasNull());
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
    getInt(getPreparedSigned(sharedConn));
    getInt(getPreparedSigned(sharedConnBinary));
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
    getIntUnsigned(getPreparedUnsigned(sharedConn));
    getIntUnsigned(getPreparedUnsigned(sharedConnBinary));
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
    getLong(getPreparedSigned(sharedConn));
    getLong(getPreparedSigned(sharedConnBinary));
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
    getLongUnsigned(getPreparedUnsigned(sharedConn));
    getLongUnsigned(getPreparedUnsigned(sharedConnBinary));
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
    getFloat(getPreparedSigned(sharedConn));
    getFloat(getPreparedSigned(sharedConnBinary));
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
    getFloatUnsigned(getPreparedUnsigned(sharedConn));
    getFloatUnsigned(getPreparedUnsigned(sharedConnBinary));
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
    getDouble(getPreparedSigned(sharedConn));
    getDouble(getPreparedSigned(sharedConnBinary));
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
    getDoubleUnsigned(getPreparedUnsigned(sharedConn));
    getDoubleUnsigned(getPreparedUnsigned(sharedConnBinary));
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
    getBigDecimal(getPreparedSigned(sharedConn));
    getBigDecimal(getPreparedSigned(sharedConnBinary));
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
    getBigDecimalUnsigned(getPreparedUnsigned(sharedConn));
    getBigDecimalUnsigned(getPreparedUnsigned(sharedConnBinary));
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
    getBigIntegerUnsigned(getPreparedUnsigned(sharedConn));
    getBigIntegerUnsigned(getPreparedUnsigned(sharedConnBinary));
  }

  private void getBigIntegerUnsigned(ResultSet res) throws SQLException {
    CompleteResult rs = (CompleteResult) res;
    assertEquals(BigInteger.ZERO, rs.getBigInteger(1));
    assertFalse(rs.wasNull());
    assertEquals(BigInteger.ONE, rs.getBigInteger(2));
    assertEquals(BigInteger.ONE, rs.getBigInteger("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(BigInteger.valueOf(4294967295L), rs.getBigInteger(3));
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
    getDate(getPreparedSigned(sharedConn));
    getDate(getPreparedSigned(sharedConnBinary));
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
    getTime(getPreparedSigned(sharedConn));
    getTime(getPreparedSigned(sharedConnBinary));
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
    getTimestamp(getPreparedSigned(sharedConn));
    getTimestamp(getPreparedSigned(sharedConnBinary));
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
    getAsciiStream(getPreparedSigned(sharedConn));
    getAsciiStream(getPreparedSigned(sharedConnBinary));
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
    getUnicodeStream(getPreparedSigned(sharedConn));
    getUnicodeStream(getPreparedSigned(sharedConnBinary));
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
    getBinaryStream(getPreparedSigned(sharedConn));
    getBinaryStream(getPreparedSigned(sharedConnBinary));
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
    getBytes(getPreparedSigned(sharedConn));
    getBytes(getPreparedSigned(sharedConnBinary));
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
    getCharacterStream(getPreparedSigned(sharedConn));
    getCharacterStream(getPreparedSigned(sharedConnBinary));
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
    getNCharacterStream(getPreparedSigned(sharedConn));
    getNCharacterStream(getPreparedSigned(sharedConnBinary));
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
    getRef(getPreparedSigned(sharedConn));
    getRef(getPreparedSigned(sharedConnBinary));
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
    getBlob(getPreparedSigned(sharedConn));
    getBlob(getPreparedSigned(sharedConnBinary));
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
    getClob(getPreparedSigned(sharedConn));
    getClob(getPreparedSigned(sharedConnBinary));
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
    getNClob(getPreparedSigned(sharedConn));
    getNClob(getPreparedSigned(sharedConnBinary));
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
    getArray(getPreparedSigned(sharedConn));
    getArray(getPreparedSigned(sharedConnBinary));
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
    getURL(getPreparedSigned(sharedConn));
    getURL(getPreparedSigned(sharedConnBinary));
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
    getSQLXML(getPreparedSigned(sharedConn));
    getSQLXML(getPreparedSigned(sharedConnBinary));
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

  @Test
  public void sendParam() throws SQLException {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE IntCodec2");
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO IntCodec2 VALUES (?)")) {
      prep.setInt(1, 1);
      prep.execute();
      prep.setObject(1, 2);
      prep.execute();
      prep.setObject(1, null);
      prep.execute();
      prep.setObject(1, 3, Types.INTEGER);
      prep.execute();
      prep.setObject(1, null, Types.INTEGER);
      prep.execute();
    }

    ResultSet rs = stmt.executeQuery("SELECT * FROM IntCodec2");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(0, rs.getInt(1));
    assertTrue(rs.wasNull());
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(0, rs.getInt(1));
    assertTrue(rs.wasNull());
  }
}
