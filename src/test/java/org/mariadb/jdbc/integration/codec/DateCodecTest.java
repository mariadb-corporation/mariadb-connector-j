package org.mariadb.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.*;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;

public class DateCodecTest extends CommonCodecTest {
  @AfterAll
  public static void after2() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE DateCodec");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS DateCodec");
    stmt.execute("CREATE TABLE DateCodec (t1 DATE, t2 DATE, t3 DATE, t4 DATE)");
    stmt.execute("INSERT INTO DateCodec VALUES ('2010-01-12', '1000-01-01', '9999-12-31', null)");
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from DateCodec");
    assertTrue(rs.next());
    return rs;
  }

  private ResultSet getPrepare() throws SQLException {
    PreparedStatement stmt =
        sharedConn.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from DateCodec"
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
    getObject(getPrepare());
  }

  public void getObject(ResultSet rs) throws SQLException {
    assertFalse(rs.wasNull());
    assertEquals(Date.valueOf("2010-01-12").getTime(), ((Date) rs.getObject(1)).getTime());
    assertFalse(rs.wasNull());
    assertFalse(rs.wasNull());
    assertEquals(Date.valueOf("1000-01-01").getTime(), ((Date) rs.getObject(2)).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Date.valueOf("9999-12-31").getTime(), ((Date) rs.getObject(3)).getTime());
    assertFalse(rs.wasNull());
    assertNull(rs.getDate(4));
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
    testObject(rs, String.class, "2010-01-12");
    testErrObject(rs, Long.class);
    testErrObject(rs, Short.class);
    testErrObject(rs, BigDecimal.class);
    testErrObject(rs, BigInteger.class);
    testErrObject(rs, Double.class);
    testErrObject(rs, Float.class);
    testErrObject(rs, Byte.class);
    testErrObject(rs, byte[].class);
    testErrObject(rs, Boolean.class);
    testErrObject(rs, Clob.class);
    testErrObject(rs, NClob.class);
    testErrObject(rs, InputStream.class);
    testErrObject(rs, Reader.class);
    testObject(rs, LocalDate.class, LocalDate.parse("2010-01-12"));
    testObject(rs, LocalDateTime.class, LocalDateTime.parse("2010-01-12T00:00:00"));
    testErrObject(rs, LocalTime.class);
    testErrObject(rs, Time.class);
    testObject(rs, Timestamp.class, Timestamp.valueOf("2010-01-12 00:00:00"));
    testObject(
        rs,
        ZonedDateTime.class,
        LocalDateTime.parse("2010-01-12T00:00:00").atZone(ZoneId.systemDefault()));
    testObject(rs, java.util.Date.class, Timestamp.valueOf("2010-01-12 00:00:00.0"));
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
    assertEquals("2010-01-12", rs.getString(1));
    assertFalse(rs.wasNull());
    assertEquals("1000-01-01", rs.getString(2));
    assertEquals("1000-01-01", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("9999-12-31", rs.getString(3));
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
    assertEquals("2010-01-12", rs.getNString(1));
    assertFalse(rs.wasNull());
    assertEquals("1000-01-01", rs.getNString(2));
    assertEquals("1000-01-01", rs.getNString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("9999-12-31", rs.getNString(3));
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
        SQLException.class, () -> rs.getBoolean(1), "Data type DATE cannot be decoded as Boolean");
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
    assertThrowsContains(
        SQLException.class, () -> rs.getByte(1), "Data type DATE cannot be decoded as Byte");
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
        SQLException.class, () -> rs.getShort(1), "Data type DATE cannot be decoded as Short");
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
        SQLException.class, () -> rs.getInt(1), "Data type DATE cannot be decoded as Integer");
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
        SQLException.class, () -> rs.getLong(1), "Data type DATE cannot be decoded as Long");
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
        SQLException.class, () -> rs.getFloat(1), "Data type DATE cannot be decoded as Float");
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
        SQLException.class, () -> rs.getDouble(1), "Data type DATE cannot be decoded as Double");
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
        SQLException.class,
        () -> rs.getBigDecimal(1),
        "Data type DATE cannot be decoded as BigDecimal");
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
    assertEquals(
        1263254400000L, rs.getDate(1, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Date.valueOf("2010-01-12").getTime(), rs.getDate(1).getTime());
    assertFalse(rs.wasNull());
    assertEquals(
        -30609792000000L,
        rs.getDate(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertEquals(
        -30609792000000L,
        rs.getDate("t2alias", Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Date.valueOf("1000-01-01").getTime(), rs.getDate(2).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Date.valueOf("9999-12-31").getTime(), rs.getDate(3).getTime());
    assertFalse(rs.wasNull());
    assertNull(rs.getDate(4));
    assertTrue(rs.wasNull());
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
        SQLException.class, () -> rs.getTime(1), "Data type DATE cannot be decoded as Time");
  }

  @Test
  public void getDuration() throws SQLException {
    getDuration(get());
  }

  @Test
  public void getDurationPrepare() throws SQLException {
    getDuration(getPrepare());
  }

  public void getDuration(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class,
        () -> rs.getObject(1, Duration.class),
        "Type class java.time.Duration not supported type for DATE type");
  }

  @Test
  public void getLocalTime() throws SQLException {
    getLocalTime(get());
  }

  @Test
  public void getLocalTimePrepare() throws SQLException {
    getLocalTime(getPrepare());
  }

  public void getLocalTime(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class,
        () -> rs.getObject(1, LocalTime.class),
        "Type class java.time.LocalTime not supported type for DATE type");
  }

  @Test
  public void getLocalDate() throws SQLException {
    getLocalDate(get());
  }

  @Test
  public void getLocalDatePrepare() throws SQLException {
    getLocalDate(getPrepare());
  }

  public void getLocalDate(ResultSet rs) throws SQLException {
    assertEquals(LocalDate.parse("2010-01-12"), rs.getObject(1, LocalDate.class));
    assertFalse(rs.wasNull());
    assertEquals(LocalDate.parse("1000-01-01"), rs.getObject(2, LocalDate.class));
    assertEquals(LocalDate.parse("1000-01-01"), rs.getObject("t2alias", LocalDate.class));
    assertFalse(rs.wasNull());
    assertEquals(LocalDate.parse("9999-12-31"), rs.getObject(3, LocalDate.class));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4, LocalDate.class));
    assertTrue(rs.wasNull());
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
    assertEquals(Timestamp.valueOf("2010-01-12 00:00:00").getTime(), rs.getTimestamp(1).getTime());
    assertEquals(
        1263254400000L,
        rs.getTimestamp(1, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertEquals(
        Timestamp.valueOf("2010-01-12 00:00:00").getTime(), rs.getTimestamp("t1alias").getTime());
    assertEquals(
        1263254400000L,
        rs.getTimestamp("t1alias", Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Timestamp.valueOf("1000-01-01 00:00:00").getTime(), rs.getTimestamp(2).getTime());
    assertEquals(
        -30609792000000L,
        rs.getTimestamp(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());

    assertEquals(Timestamp.valueOf("9999-12-31 00:00:00").getTime(), rs.getTimestamp(3).getTime());
    assertEquals(
        253402214400000L,
        rs.getTimestamp(3, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Timestamp.valueOf("9999-12-31 00:00:00"), rs.getTimestamp(3));
    assertNull(rs.getTimestamp(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getAsciiStream() throws SQLException {
    getAsciiStream(get());
  }

  @Test
  public void getAsciiStreamPrepare() throws SQLException {
    getAsciiStream(getPrepare());
  }

  public void getAsciiStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class,
        () -> rs.getAsciiStream(1),
        "Data type DATE cannot be decoded as Stream");
  }

  @Test
  public void getUnicodeStream() throws SQLException {
    getUnicodeStream(get());
  }

  @Test
  public void getUnicodeStreamPrepare() throws SQLException {
    getUnicodeStream(getPrepare());
  }

  @SuppressWarnings("deprecation")
  public void getUnicodeStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class,
        () -> rs.getUnicodeStream(1),
        "Data type DATE cannot be decoded as Stream");
  }

  @Test
  public void getBinaryStream() throws SQLException {
    getBinaryStream(get());
  }

  @Test
  public void getBinaryStreamPrepare() throws SQLException {
    getBinaryStream(getPrepare());
  }

  public void getBinaryStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class,
        () -> rs.getBinaryStream(1),
        "Data type DATE cannot be decoded as Stream");
  }

  @Test
  public void getBytes() throws SQLException {
    getBytes(get());
  }

  @Test
  public void getBytesPrepare() throws SQLException {
    getBytes(getPrepare());
  }

  public void getBytes(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getBytes(1), "Data type DATE cannot be decoded as byte[]");
  }

  @Test
  public void getCharacterStream() throws SQLException {
    getCharacterStream(get());
  }

  @Test
  public void getCharacterStreamPrepare() throws SQLException {
    getCharacterStream(getPrepare());
  }

  public void getCharacterStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class,
        () -> rs.getCharacterStream(1),
        "Data type DATE cannot be decoded as Reader");
  }

  @Test
  public void getNCharacterStream() throws SQLException {
    getNCharacterStream(get());
  }

  @Test
  public void getNCharacterStreamPrepare() throws SQLException {
    getNCharacterStream(getPrepare());
  }

  public void getNCharacterStream(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream(1),
        "Data type DATE cannot be decoded as Reader");
  }

  @Test
  public void getBlob() throws SQLException {
    getBlob(get());
  }

  @Test
  public void getBlobPrepare() throws SQLException {
    getBlob(getPrepare());
  }

  public void getBlob(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getBlob(1), "Data type DATE cannot be decoded as Blob");
  }

  @Test
  public void getClob() throws SQLException {
    getClob(get());
  }

  @Test
  public void getClobPrepare() throws SQLException {
    getClob(getPrepare());
  }

  public void getClob(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getClob(1), "Data type DATE cannot be decoded as Clob");
  }

  @Test
  public void getNClob() throws SQLException {
    getNClob(get());
  }

  @Test
  public void getNClobPrepare() throws SQLException {
    getNClob(getPrepare());
  }

  public void getNClob(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class, () -> rs.getNClob(1), "Data type DATE cannot be decoded as Clob");
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = get();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("DATE", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.sql.Date", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.DATE, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(10, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(10, meta.getColumnDisplaySize(1));
  }
}
