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
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.integration.Common;

public class DateCodecTest extends CommonCodecTest {
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS DateCodec");
    stmt.execute("DROP TABLE IF EXISTS DateCodec2");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE DateCodec (t1 DATE, t2 DATE, t3 DATE, t4 DATE)");
    stmt.execute(
        "INSERT INTO DateCodec VALUES ('2010-01-12', '1000-01-01', '9999-12-31', null)"
            + (isMariaDBServer() ? ",('0000-00-00', '1000-01-01', '9999-12-31', null)" : ""));
    stmt.execute("CREATE TABLE DateCodec2 (id int not null primary key auto_increment, t1 DATE)");
    stmt.execute("FLUSH TABLES");
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from DateCodec");
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private ResultSet getPrepare(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    PreparedStatement preparedStatement =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from DateCodec"
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
    if (isMariaDBServer()) {
      assertTrue(rs.next());
      assertNull(rs.getObject(1));
      assertTrue(rs.wasNull());
      assertNull(rs.getObject(1, LocalDateTime.class));
      assertTrue(rs.wasNull());
      assertNull(rs.getObject(1, ZonedDateTime.class));
      assertTrue(rs.wasNull());
      assertNull(rs.getObject(1, LocalDate.class));
      assertTrue(rs.wasNull());
    }
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
    testErrObject(rs, Duration.class);
    testErrObject(rs, Clob.class);
    testErrObject(rs, NClob.class);
    testErrObject(rs, InputStream.class);
    testErrObject(rs, Reader.class);
    testObject(rs, LocalDate.class, LocalDate.parse("2010-01-12"));
    testObject(rs, LocalDateTime.class, LocalDateTime.parse("2010-01-12T00:00:00"));
    testErrObject(rs, LocalTime.class);
    testErrObject(rs, Time.class);
    testErrObject(rs, BigInteger.class);
    testObject(rs, Timestamp.class, Timestamp.valueOf("2010-01-12 00:00:00"));
    testObject(
        rs,
        ZonedDateTime.class,
        LocalDateTime.parse("2010-01-12T00:00:00").atZone(ZoneId.systemDefault()));
    testObject(rs, java.util.Date.class, Date.valueOf("2010-01-12"));
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
    assertEquals("2010-01-12", rs.getString(1));
    assertFalse(rs.wasNull());
    assertEquals("1000-01-01", rs.getString(2));
    assertEquals("1000-01-01", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals("9999-12-31", rs.getString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getString(4));
    assertTrue(rs.wasNull());
    if (isMariaDBServer()) {
      rs.next();
      assertEquals("0000-00-00", rs.getString(1));
    }
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
    getBoolean(getPrepare(sharedConn));
    getBoolean(getPrepare(sharedConnBinary));
  }

  public void getBoolean(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class, () -> rs.getBoolean(1), "Data type DATE cannot be decoded as Boolean");
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

  public void getByte(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class, () -> rs.getByte(1), "Data type DATE cannot be decoded as Byte");
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

  public void getShort(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class, () -> rs.getShort(1), "Data type DATE cannot be decoded as Short");
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

  public void getInt(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class, () -> rs.getInt(1), "Data type DATE cannot be decoded as Integer");
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

  public void getLong(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class, () -> rs.getLong(1), "Data type DATE cannot be decoded as Long");
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
        SQLException.class, () -> rs.getFloat(1), "Data type DATE cannot be decoded as Float");
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
        SQLException.class, () -> rs.getDouble(1), "Data type DATE cannot be decoded as Double");
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

  public void getBigDecimal(ResultSet rs) {
    Common.assertThrowsContains(
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
    getDate(getPrepare(sharedConn));
    getDate(getPrepare(sharedConnBinary));
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
    if (isMariaDBServer()) {
      rs.next();
      assertNull(rs.getDate(1));
    }
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
        SQLException.class, () -> rs.getTime(1), "Data type DATE cannot be decoded as Time");
  }

  @Test
  public void getDuration() throws SQLException {
    getDuration(get());
  }

  @Test
  public void getDurationPrepare() throws SQLException {
    getDuration(getPrepare(sharedConn));
    getDuration(getPrepare(sharedConnBinary));
  }

  public void getDuration(ResultSet rs) {
    Common.assertThrowsContains(
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
    getLocalTime(getPrepare(sharedConn));
    getLocalTime(getPrepare(sharedConnBinary));
  }

  public void getLocalTime(ResultSet rs) {
    Common.assertThrowsContains(
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
    getLocalDate(getPrepare(sharedConn));
    getLocalDate(getPrepare(sharedConnBinary));
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
    if (isMariaDBServer()) {
      rs.next();
      assertNull(rs.getObject(1, LocalDate.class));
    }
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
    if (isMariaDBServer()) {
      rs.next();
      assertNull(rs.getTimestamp(1));
    }
  }

  @Test
  public void getLocalDateTime() throws SQLException {
    getLocalDateTime(get());
  }

  @Test
  public void getLocalDateTimePrepare() throws SQLException {
    getLocalDateTime(getPrepare(sharedConn));
    getLocalDateTime(getPrepare(sharedConnBinary));
  }

  public void getLocalDateTime(ResultSet rs) throws SQLException {
    assertEquals(LocalDateTime.parse("2010-01-12T00:00:00"), rs.getObject(1, LocalDateTime.class));
    assertEquals(
        LocalDateTime.parse("2010-01-12T00:00:00"), rs.getObject("t1alias", LocalDateTime.class));
    assertNull(rs.getObject(4, LocalDateTime.class));
    assertTrue(rs.wasNull());
    if (isMariaDBServer()) {
      rs.next();
      assertNull(rs.getObject(1, LocalDateTime.class));
    }
  }

  @Test
  public void getInstant() throws SQLException {
    getInstant(get());
  }

  @Test
  public void getInstantPrepare() throws SQLException {
    getInstant(getPrepare(sharedConn));
    getInstant(getPrepare(sharedConnBinary));
  }

  public void getInstant(ResultSet rs) throws SQLException {
    assertFalse(rs.wasNull());
    assertEquals(
        ZonedDateTime.of(LocalDateTime.parse("2010-01-12T00:00:00"), ZoneId.systemDefault())
            .toInstant(),
        rs.getObject(1, Instant.class));
    assertFalse(rs.wasNull());
    assertEquals(
        ZonedDateTime.of(LocalDateTime.parse("1000-01-01T00:00:00"), ZoneId.systemDefault())
            .toInstant(),
        rs.getObject(2, Instant.class));
    assertFalse(rs.wasNull());
    assertEquals(
        ZonedDateTime.of(LocalDateTime.parse("9999-12-31T00:00:00"), ZoneId.systemDefault())
            .toInstant(),
        rs.getObject(3, Instant.class));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4, Instant.class));
    assertTrue(rs.wasNull());
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
        "cannot be decoded as OffsetDateTime");
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
    getUnicodeStream(getPrepare(sharedConn));
    getUnicodeStream(getPrepare(sharedConnBinary));
  }

  @SuppressWarnings("deprecation")
  public void getUnicodeStream(ResultSet rs) {
    Common.assertThrowsContains(
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
    getBinaryStream(getPrepare(sharedConn));
    getBinaryStream(getPrepare(sharedConnBinary));
  }

  public void getBinaryStream(ResultSet rs) {
    Common.assertThrowsContains(
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
    getBytes(getPrepare(sharedConn));
    getBytes(getPrepare(sharedConnBinary));
  }

  public void getBytes(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class, () -> rs.getBytes(1), "Data type DATE cannot be decoded as byte[]");
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
    getNCharacterStream(getPrepare(sharedConn));
    getNCharacterStream(getPrepare(sharedConnBinary));
  }

  public void getNCharacterStream(ResultSet rs) {
    Common.assertThrowsContains(
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
    getBlob(getPrepare(sharedConn));
    getBlob(getPrepare(sharedConnBinary));
  }

  public void getBlob(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class, () -> rs.getBlob(1), "Data type DATE cannot be decoded as Blob");
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
        SQLException.class, () -> rs.getClob(1), "Data type DATE cannot be decoded as Clob");
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

  @Test
  public void sendParam() throws SQLException {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE DateCodec2");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO DateCodec2(t1) VALUES (?)")) {
      prep.setDate(1, Date.valueOf("2010-01-12"));
      prep.execute();
      prep.setDate(1, null);
      prep.execute();
      prep.setObject(1, Date.valueOf("2010-01-13"));
      prep.execute();
      prep.setObject(1, null);
      prep.execute();
      prep.setObject(1, Date.valueOf("2010-01-14"), Types.DATE);
      prep.execute();
      prep.setObject(1, null, Types.DATE);
      prep.execute();
      prep.setObject(1, LocalDate.parse("9999-12-31"), Types.DATE);
      prep.execute();
      prep.setDate(1, Date.valueOf("2010-01-12"), Calendar.getInstance());
      prep.execute();
      prep.setObject(1, new java.util.Date(Date.valueOf("2010-12-13").getTime()));
      prep.execute();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM DateCodec2");
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2010-01-12"), rs.getDate(2));
    rs.updateDate(2, Date.valueOf("2021-01-12"));
    rs.updateRow();
    assertEquals(Date.valueOf("2021-01-12"), rs.getDate(2));
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    rs.updateDate("t1", Date.valueOf("2021-01-15"));
    rs.updateRow();
    assertEquals(Date.valueOf("2021-01-15"), rs.getDate(2));
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2010-01-13"), rs.getDate(2));
    rs.updateDate(2, null);
    rs.updateRow();
    assertNull(rs.getString(2));
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    rs.updateObject(2, Date.valueOf("2021-01-14"), Types.DATE);
    rs.updateRow();
    assertEquals(Date.valueOf("2021-01-14"), rs.getDate(2));
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2010-01-14"), rs.getDate(2));
    rs.updateObject(2, null, Types.DATE);
    rs.updateRow();
    assertNull(rs.getString(2));
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    rs.updateObject(2, LocalDate.parse("9999-12-31"), Types.DATE);
    rs.updateRow();
    assertEquals(Date.valueOf("9999-12-31"), rs.getDate(2));
    assertTrue(rs.next());
    assertEquals(Date.valueOf("9999-12-31"), rs.getDate(2));
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2010-01-12"), rs.getDate(2));
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2010-12-13"), rs.getDate(2));
    rs.updateObject(2, new java.util.Date(Date.valueOf("2010-12-31").getTime()), Types.DATE);
    rs.updateRow();

    rs = stmt.executeQuery("SELECT * FROM DateCodec2");
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2021-01-12"), rs.getDate(2));
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2021-01-15"), rs.getDate(2));
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2021-01-14"), rs.getDate(2));
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    assertTrue(rs.next());
    assertEquals(Date.valueOf("9999-12-31"), rs.getDate(2));
    assertTrue(rs.next());
    assertEquals(Date.valueOf("9999-12-31"), rs.getDate(2));
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2010-01-12"), rs.getDate(2));
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2010-12-31"), rs.getDate(2));
    con.commit();
  }
}
