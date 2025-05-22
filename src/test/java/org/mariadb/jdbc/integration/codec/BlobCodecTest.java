// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.*;
import java.util.Arrays;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.result.CompleteResult;
import org.mariadb.jdbc.integration.Common;

public class BlobCodecTest extends CommonCodecTest {
  private static final byte[] fileContent = new byte[11000];
  private static File tmpFile;

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS BlobCodec");
    stmt.execute("DROP TABLE IF EXISTS BlobCodec2");
  }

  @BeforeAll
  public static void beforeAll2() throws Exception {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE BlobCodec (t1 TINYBLOB, t2 TINYBLOB, t3 TINYBLOB, t4 TINYBLOB)");
    stmt.execute(
        "INSERT INTO BlobCodec VALUES ('0', '1', 'some🌟', null), "
            + "('2011-01-01', '2010-12-31 23:59:59.152', '23:54:51.840010', null),"
            + "('', '2010-12-31 23:59:59.152', '23:54:51.840010', null)");
    stmt.execute("CREATE TABLE BlobCodec2 (id int not null primary key auto_increment, t1 BLOB)");
    stmt.execute("FLUSH TABLES");

    tmpFile = File.createTempFile("temp-file-name", ".tmp");
    for (int i = 0; i < 11_000; i++) {
      fileContent[i] = (byte) (i % 110 + 40);
    }

    try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
      fos.write(fileContent);
    }
  }

  private ResultSet get() throws SQLException {
    Statement stmt =
        sharedConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from BlobCodec");
    assertTrue(rs.next());
    return rs;
  }

  private CompleteResult getPrepare(org.mariadb.jdbc.Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    PreparedStatement preparedStatement =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from BlobCodec"
                + " WHERE 1 > ?",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
    preparedStatement.closeOnCompletion();
    preparedStatement.setInt(1, 0);
    CompleteResult rs = (CompleteResult) preparedStatement.executeQuery();
    assertTrue(rs.next());
    con.commit();
    return rs;
  }

  @Test
  void getObject() throws Exception {
    getObject(get());
  }

  @Test
  void getObjectPrepare() throws Exception {
    getObject(getPrepare(sharedConn));
    getObject(getPrepare(sharedConnBinary));
  }

  void getObject(ResultSet rs) throws Exception {
    assertArrayEquals("0".getBytes(), (byte[]) rs.getObject(1));
    assertFalse(rs.wasNull());
    assertArrayEquals("1".getBytes(), (byte[]) rs.getObject(2));
    if (!defaultOther.contains("useSequentialAccess")) {
      assertArrayEquals("1".getBytes(), (byte[]) rs.getObject("t2alias"));
    }
    assertFalse(rs.wasNull());
    assertArrayEquals("some🌟".getBytes(StandardCharsets.UTF_8), (byte[]) rs.getObject(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4));
    assertTrue(rs.wasNull());
  }

  @Test
  void getObjectBlob() throws Exception {
    getObjectBlob(get());
  }

  @Test
  void getObjectBlobPrepare() throws Exception {
    getObjectBlob(getPrepare(sharedConn));
    getObjectBlob(getPrepare(sharedConnBinary));
  }

  void getObjectBlob(ResultSet rs) throws Exception {
    assertStreamEquals(new MariaDbBlob("0".getBytes()), rs.getBlob(1));
    assertFalse(rs.wasNull());
    assertStreamEquals(new MariaDbBlob("1".getBytes()), rs.getBlob(2));
    if (!defaultOther.contains("useSequentialAccess")) {
      assertStreamEquals(new MariaDbBlob("1".getBytes()), rs.getBlob("t2alias"));
    }
    assertFalse(rs.wasNull());
    assertStreamEquals(new MariaDbBlob("some🌟".getBytes(StandardCharsets.UTF_8)), rs.getBlob(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4));
    assertTrue(rs.wasNull());
  }

  @Test
  void getObjectType() throws Exception {
    Assumptions.assumeFalse(defaultOther.contains("useSequentialAccess"));
    getObjectType(get());
  }

  @Test
  void getObjectTypePrepare() throws Exception {
    getObjectType(getPrepare(sharedConn));
    getObjectType(getPrepare(sharedConnBinary));
  }

  void getObjectType(ResultSet rs) throws Exception {
    testErrObject(rs, Integer.class);
    testObject(rs, String.class, "0");
    testErrObject(rs, Long.class);
    testErrObject(rs, Short.class);
    testErrObject(rs, BigDecimal.class);
    testErrObject(rs, BigInteger.class);
    testErrObject(rs, Double.class);
    testErrObject(rs, Float.class);
    testObject(rs, Byte.class, (byte) '0');
    testArrObject(rs, new byte[] {(byte) '0'});
    testErrObject(rs, Boolean.class);
    testErrObject(rs, Clob.class);
    testErrObject(rs, NClob.class);
    testObject(rs, InputStream.class, new MariaDbBlob("0".getBytes()).getBinaryStream());
    testErrObject(rs, Reader.class);
    rs.next();
    testErrObject(rs, LocalDate.class);
    testErrObject(rs, LocalTime.class);
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
  void getString() throws SQLException {
    getString(get());
  }

  @Test
  void getStringPrepare() throws SQLException {
    getString(getPrepare(sharedConn));
    getString(getPrepare(sharedConnBinary));
  }

  void getString(ResultSet rs) throws SQLException {
    assertEquals("0", rs.getString(1));
    assertFalse(rs.wasNull());
    assertEquals("1", rs.getString(2));
    if (!defaultOther.contains("useSequentialAccess")) {
      assertEquals("1", rs.getString("t2alias"));
    }
    assertFalse(rs.wasNull());
    assertEquals("some🌟", rs.getString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getNString(4));
    assertTrue(rs.wasNull());
  }

  @Test
  void getNString() throws SQLException {
    getNString(get());
  }

  @Test
  void getNStringPrepare() throws SQLException {
    getNString(getPrepare(sharedConn));
    getNString(getPrepare(sharedConnBinary));
  }

  void getNString(ResultSet rs) throws SQLException {
    assertEquals("0", rs.getNString(1));
    assertFalse(rs.wasNull());
    assertEquals("1", rs.getNString(2));
    if (!defaultOther.contains("useSequentialAccess")) {
      assertEquals("1", rs.getNString("t2alias"));
    }
    assertFalse(rs.wasNull());
    assertEquals("some🌟", rs.getNString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getNString(4));
    assertTrue(rs.wasNull());
  }

  @Test
  void getBoolean() throws SQLException {
    getBoolean(get());
  }

  @Test
  void getBooleanPrepare() throws SQLException {
    getBoolean(getPrepare(sharedConn));
    getBoolean(getPrepare(sharedConnBinary));
  }

  void getBoolean(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBoolean(1),
        "Data type BLOB cannot be decoded as Boolean");
    assertFalse(rs.wasNull());
  }

  @Test
  void getByte() throws SQLException {
    getByte(get());
  }

  @Test
  void getBytePrepare() throws SQLException {
    getByte(getPrepare(sharedConn));
    getByte(getPrepare(sharedConnBinary));
  }

  void getByte(ResultSet rs) throws SQLException {
    assertEquals((byte) 48, rs.getByte(1));
    assertFalse(rs.wasNull());
    assertEquals((byte) 49, rs.getByte(2));
    if (!defaultOther.contains("useSequentialAccess")) {
      assertEquals((byte) 49, rs.getByte("t2alias"));
    }
    assertFalse(rs.wasNull());
    assertEquals((byte) 115, rs.getByte(3));
    assertFalse(rs.wasNull());
    assertEquals((byte) 0, rs.getByte(4));
    assertTrue(rs.wasNull());
    rs.next();
    rs.next();
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getByte(1),
        "empty String value cannot be decoded as Byte");
  }

  @Test
  void getShort() throws SQLException {
    getShort(get());
  }

  @Test
  void getShortPrepare() throws SQLException {
    getShort(getPrepare(sharedConn));
    getShort(getPrepare(sharedConnBinary));
  }

  void getShort(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getShort(1), "Data type BLOB cannot be decoded as Short");
    assertFalse(rs.wasNull());
  }

  @Test
  void getInt() throws SQLException {
    getInt(get());
  }

  @Test
  void getIntPrepare() throws SQLException {
    getInt(getPrepare(sharedConn));
    getInt(getPrepare(sharedConnBinary));
  }

  void getInt(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getInt(1), "Data type BLOB cannot be decoded as Integer");
    assertFalse(rs.wasNull());
  }

  @Test
  void getLong() throws SQLException {
    getLong(get());
  }

  @Test
  void getLongPrepare() throws SQLException {
    getLong(getPrepare(sharedConn));
    getLong(getPrepare(sharedConnBinary));
  }

  void getLong(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getLong(1), "Data type BLOB cannot be decoded as Long");
    assertFalse(rs.wasNull());
  }

  @Test
  void getFloat() throws SQLException {
    getFloat(get());
  }

  @Test
  void getFloatPrepare() throws SQLException {
    getFloat(getPrepare(sharedConn));
    getFloat(getPrepare(sharedConnBinary));
  }

  void getFloat(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getFloat(1), "Data type BLOB cannot be decoded as Float");
    assertFalse(rs.wasNull());
  }

  @Test
  void getDouble() throws SQLException {
    getDouble(get());
  }

  @Test
  void getDoublePrepare() throws SQLException {
    getDouble(getPrepare(sharedConn));
    getDouble(getPrepare(sharedConnBinary));
  }

  void getDouble(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getDouble(1),
        "Data type BLOB cannot be decoded as Double");
    assertFalse(rs.wasNull());
  }

  @Test
  void getBigDecimal() throws SQLException {
    getBigDecimal(get());
  }

  @Test
  void getBigDecimalPrepare() throws SQLException {
    getBigDecimal(getPrepare(sharedConn));
    getBigDecimal(getPrepare(sharedConnBinary));
  }

  void getBigDecimal(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBigDecimal(1),
        "Data type BLOB cannot be decoded as BigDecimal");
    assertFalse(rs.wasNull());
  }

  @Test
  void getBigIntegerPrepare() throws SQLException {
    getBigInteger(getPrepare(sharedConn));
    getBigInteger(getPrepare(sharedConnBinary));
  }

  void getBigInteger(CompleteResult rs) {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBigInteger(1),
        "Data type BLOB cannot be decoded as BigInteger");
    assertFalse(rs.wasNull());
  }

  @Test
  void getDuration() throws SQLException {
    getDuration(get());
  }

  @Test
  void getDurationPrepare() throws SQLException {
    getDuration(getPrepare(sharedConn));
    getDuration(getPrepare(sharedConnBinary));
  }

  void getDuration(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class,
        () -> rs.getObject(1, Duration.class),
        "Data type BLOB cannot be decoded as Duration");
  }

  @Test
  void getDate() throws SQLException {
    getDate(get());
  }

  @Test
  void getDatePrepare() throws SQLException {
    getDate(getPrepare(sharedConn));
    getDate(getPrepare(sharedConnBinary));
  }

  void getDate(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getDate(1), "Data type BLOB cannot be decoded as Date");
    assertFalse(rs.wasNull());
  }

  @Test
  void getTime() throws SQLException {
    getTime(get());
  }

  @Test
  void getTimePrepare() throws SQLException {
    getTime(getPrepare(sharedConn));
    getTime(getPrepare(sharedConnBinary));
  }

  void getTime(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getTime(1), "Data type BLOB cannot be decoded as Time");
    assertFalse(rs.wasNull());
  }

  @Test
  void getTimestamp() throws SQLException {
    getTimestamp(get());
  }

  @Test
  void getTimestampPrepare() throws SQLException {
    getTimestamp(getPrepare(sharedConn));
    getTimestamp(getPrepare(sharedConnBinary));
  }

  void getTimestamp(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getTimestamp(1),
        "Data type BLOB cannot be decoded as Timestamp");
    assertFalse(rs.wasNull());
  }

  @Test
  void getOffsetDateTime() throws SQLException {
    getOffsetDateTime(get());
  }

  @Test
  void getOffsetDateTimePrepare() throws SQLException {
    getOffsetDateTime(getPrepare(sharedConn));
    getOffsetDateTime(getPrepare(sharedConnBinary));
  }

  void getOffsetDateTime(ResultSet rs) {
    Common.assertThrowsContains(
        SQLException.class,
        () -> rs.getObject(1, OffsetDateTime.class),
        "cannot be decoded as OffsetDateTime");
  }

  @Test
  void getAsciiStream() throws Exception {
    getAsciiStream(get());
  }

  @Test
  void getAsciiStreamPrepare() throws Exception {
    getAsciiStream(getPrepare(sharedConn));
    getAsciiStream(getPrepare(sharedConnBinary));
  }

  void getAsciiStream(ResultSet rs) throws Exception {
    assertStreamEquals(new ByteArrayInputStream("0".getBytes()), rs.getAsciiStream(1));
    assertFalse(rs.wasNull());
    assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getAsciiStream(2));
    if (!defaultOther.contains("useSequentialAccess")) {
      assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getAsciiStream("t2alias"));
    }
    assertFalse(rs.wasNull());
    assertStreamEquals(
        new ByteArrayInputStream("some🌟".getBytes(StandardCharsets.UTF_8)), rs.getAsciiStream(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getAsciiStream(4));
    assertTrue(rs.wasNull());
  }

  @Test
  void getUnicodeStream() throws Exception {
    getUnicodeStream(get());
  }

  @Test
  void getUnicodeStreamPrepare() throws Exception {
    getUnicodeStream(getPrepare(sharedConn));
    getUnicodeStream(getPrepare(sharedConnBinary));
  }

  @SuppressWarnings("deprecation")
  void getUnicodeStream(ResultSet rs) throws Exception {
    assertStreamEquals(new ByteArrayInputStream("0".getBytes()), rs.getUnicodeStream(1));
    assertFalse(rs.wasNull());
    assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getUnicodeStream(2));
    if (!defaultOther.contains("useSequentialAccess")) {
      assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getUnicodeStream("t2alias"));
    }
    assertFalse(rs.wasNull());
    assertStreamEquals(
        new ByteArrayInputStream("some🌟".getBytes(StandardCharsets.UTF_8)),
        rs.getUnicodeStream(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getUnicodeStream(4));
    assertTrue(rs.wasNull());
  }

  @Test
  void getBinaryStream() throws Exception {
    getBinaryStream(get());
  }

  @Test
  void getBinaryStreamPrepare() throws Exception {
    getBinaryStream(getPrepare(sharedConn));
    getBinaryStream(getPrepare(sharedConnBinary));
  }

  void getBinaryStream(ResultSet rs) throws Exception {
    assertStreamEquals(new ByteArrayInputStream("0".getBytes()), rs.getBinaryStream(1));
    assertFalse(rs.wasNull());
    assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getBinaryStream(2));
    if (!defaultOther.contains("useSequentialAccess")) {
      assertStreamEquals(new ByteArrayInputStream("1".getBytes()), rs.getBinaryStream("t2alias"));
    }
    assertFalse(rs.wasNull());
    assertStreamEquals(
        new ByteArrayInputStream("some🌟".getBytes(StandardCharsets.UTF_8)), rs.getBinaryStream(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBinaryStream(4));
    assertTrue(rs.wasNull());
  }

  @Test
  void getBytes() throws Exception {
    getBytes(get());
  }

  @Test
  void getBytesPrepare() throws Exception {
    getBytes(getPrepare(sharedConn));
    getBytes(getPrepare(sharedConnBinary));
  }

  void getBytes(ResultSet rs) throws Exception {
    assertArrayEquals("0".getBytes(), rs.getBytes(1));
    assertFalse(rs.wasNull());
    assertArrayEquals("1".getBytes(), rs.getBytes(2));
    if (!defaultOther.contains("useSequentialAccess")) {
      assertArrayEquals("1".getBytes(), rs.getBytes("t2alias"));
    }
    assertFalse(rs.wasNull());
    assertArrayEquals("some🌟".getBytes(StandardCharsets.UTF_8), rs.getBytes(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBytes(4));
    assertTrue(rs.wasNull());
  }

  @Test
  void getCharacterStream() throws Exception {
    getCharacterStream(get());
  }

  @Test
  void getCharacterStreamPrepare() throws Exception {
    getCharacterStream(getPrepare(sharedConn));
    getCharacterStream(getPrepare(sharedConnBinary));
  }

  void getCharacterStream(ResultSet rs) throws Exception {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getCharacterStream(1),
        "Data type BLOB cannot be decoded as Reader");
    assertFalse(rs.wasNull());
  }

  @Test
  void getNCharacterStream() throws Exception {
    getNCharacterStream(get());
  }

  @Test
  void getNCharacterStreamPrepare() throws Exception {
    getNCharacterStream(getPrepare(sharedConn));
    getNCharacterStream(getPrepare(sharedConnBinary));
  }

  void getNCharacterStream(ResultSet rs) throws Exception {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNCharacterStream(1),
        "Data type BLOB cannot be decoded as Reader");
    assertFalse(rs.wasNull());
  }

  @Test
  void getBlob() throws Exception {
    getBlob(get());
  }

  @Test
  void getBlobPrepare() throws Exception {
    getBlob(getPrepare(sharedConn));
    getBlob(getPrepare(sharedConnBinary));
  }

  void getBlob(ResultSet rs) throws Exception {
    assertStreamEquals(new MariaDbBlob("0".getBytes()), rs.getBlob(1));
    assertFalse(rs.wasNull());
    assertStreamEquals(new MariaDbBlob("1".getBytes()), rs.getBlob(2));
    if (!defaultOther.contains("useSequentialAccess")) {
      assertStreamEquals(new MariaDbBlob("1".getBytes()), rs.getBlob("t2alias"));
    }
    assertFalse(rs.wasNull());
    assertStreamEquals(new MariaDbBlob("some🌟".getBytes(StandardCharsets.UTF_8)), rs.getBlob(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getBlob(4));
    assertTrue(rs.wasNull());
  }

  @Test
  void getClob() throws Exception {
    getClob(get());
  }

  @Test
  void getClobPrepare() throws Exception {
    getClob(getPrepare(sharedConn));
    getClob(getPrepare(sharedConnBinary));
  }

  void getClob(ResultSet rs) throws Exception {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getClob(1), "Data type BLOB cannot be decoded as Clob");
    assertFalse(rs.wasNull());
  }

  @Test
  void getNClob() throws Exception {
    getNClob(get());
  }

  @Test
  void getNClobPrepare() throws Exception {
    getNClob(getPrepare(sharedConn));
    getNClob(getPrepare(sharedConnBinary));
  }

  void getNClob(ResultSet rs) throws Exception {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getNClob(1), "Data type BLOB cannot be decoded as Clob");
    assertFalse(rs.wasNull());
  }

  @Test
  void getMetaData() throws SQLException {
    ResultSet rs = get();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("TINYBLOB", meta.getColumnTypeName(1));
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
  void sendParam() throws Exception {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);

    try (Connection con = createCon("transactionReplay=true&useServerPrepStmts=false")) {
      sendParam(con);
    }
    try (Connection con = createCon("transactionReplay=true&useServerPrepStmts=true")) {
      sendParam(con);
    }
    try (Connection con = createCon()) {
      java.sql.Statement stmt = con.createStatement();
      stmt.execute("SET sql_mode = concat(@@sql_mode,',NO_BACKSLASH_ESCAPES')");
      sendParam(con);
    }
  }

  private void sendParam(Connection con) throws Exception {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE BlobCodec2");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO BlobCodec2(t1) VALUES (?)")) {
      prep.setBlob(1, (Blob) null);
      prep.execute();
      prep.setBlob(1, new MariaDbBlob("e🌟1".getBytes(StandardCharsets.UTF_8)));
      prep.execute();
      prep.setBlob(1, (Blob) null);
      prep.execute();
      prep.setNull(1, Types.BLOB);
      prep.execute();

      prep.setObject(1, new MariaDbBlob("e🌟2".getBytes(StandardCharsets.UTF_8)));
      prep.execute();
      prep.setObject(1, new MariaDbBlob("e🌟2".getBytes(StandardCharsets.UTF_8)), Types.BLOB, 5);
      prep.execute();
      prep.setObject(1, null);
      prep.execute();
      prep.setObject(1, new MariaDbBlob("e🌟3".getBytes(StandardCharsets.UTF_8)), Types.BLOB);
      prep.execute();
      prep.setObject(1, null, Types.BLOB);
      prep.execute();

      prep.setObject(1, new MariaDbBlob("e🌟4".getBytes(StandardCharsets.UTF_8)));
      prep.addBatch();
      prep.setObject(1, new MariaDbBlob("e🌟56".getBytes(StandardCharsets.UTF_8)), Types.BLOB, 6);
      prep.addBatch();
      prep.executeBatch();

      try (FileInputStream fis = new FileInputStream(tmpFile)) {
        prep.setObject(1, fis, Types.BLOB);
        prep.execute();
      }
      try (FileInputStream fis = new FileInputStream(tmpFile)) {
        prep.setObject(1, fis, Types.BLOB, 5000);
        prep.execute();
      }
      try (FileInputStream fis = new FileInputStream(tmpFile)) {
        try (FileInputStream fis2 = new FileInputStream(tmpFile)) {
          prep.setObject(1, fis, Types.BLOB);
          prep.addBatch();
          prep.setObject(1, fis2, Types.BLOB, 5000);
          prep.addBatch();
          prep.executeBatch();
        }
      }
      try (FileInputStream fis = new FileInputStream(tmpFile)) {
        prep.setBlob(1, new BlobInputStream(fis));
        prep.addBatch();
        prep.executeBatch();
      }
      try (FileInputStream fis = new FileInputStream(tmpFile)) {
        prep.setObject(1, new BlobInputStream(fis), Types.BLOB, 5000);
        prep.addBatch();
        prep.executeBatch();
      }

      prep.setObject(1, "e🌟6''".getBytes(StandardCharsets.UTF_8));
      prep.addBatch();
      prep.setObject(1, "e🌟76".getBytes(StandardCharsets.UTF_8), Types.BLOB, 6);
      prep.addBatch();
      prep.executeBatch();
      prep.setObject(1, "e🌟85".getBytes(StandardCharsets.UTF_8), Types.BLOB, 6);
      prep.execute();
      prep.setBytes(1, "e🌟9''\\n".getBytes(StandardCharsets.UTF_8));
      prep.execute();
      prep.setBinaryStream(1, new ByteArrayInputStream("e🌟9".getBytes(StandardCharsets.UTF_8)));
      prep.execute();
      prep.setBinaryStream(
          1, new ByteArrayInputStream("e🌟123".getBytes(StandardCharsets.UTF_8)), 6);
      prep.execute();
      prep.setBinaryStream(
          1, new ByteArrayInputStream("e🌟456".getBytes(StandardCharsets.UTF_8)), 6L);
      prep.execute();
      prep.setBinaryStream(1, new ByteArrayInputStream("e🌟9".getBytes(StandardCharsets.UTF_8)));
      prep.execute();
      prep.setBinaryStream(
          1, new ByteArrayInputStream("e🌟123".getBytes(StandardCharsets.UTF_8)), 6);
      prep.execute();
      prep.setBinaryStream(
          1, new ByteArrayInputStream("e🌟456".getBytes(StandardCharsets.UTF_8)), 6L);
      prep.execute();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM BlobCodec2");
    assertTrue(rs.next());
    assertNull(rs.getBlob(2));

    assertTrue(rs.next());
    assertArrayEquals(
        "e🌟1".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));
    rs.updateNull(2);
    rs.updateRow();
    assertNull(rs.getBlob(2));

    assertTrue(rs.next());
    assertNull(rs.getBlob(2));
    rs.updateBlob(2, new MariaDbBlob("g🌟1".getBytes(StandardCharsets.UTF_8)));
    rs.updateRow();
    assertArrayEquals(
        "g🌟1".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));

    assertTrue(rs.next());
    assertNull(rs.getBlob(2));
    rs.updateBlob("t1", new MariaDbBlob("f🌟1".getBytes(StandardCharsets.UTF_8)));
    rs.updateRow();
    assertArrayEquals(
        "f🌟1".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));

    assertTrue(rs.next());
    assertArrayEquals(
        "e🌟2".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));
    rs.updateNull("t1");
    rs.updateRow();
    assertNull(rs.getBlob(2));

    assertTrue(rs.next());
    assertArrayEquals(
        "e🌟".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));
    rs.updateObject(2, new MariaDbBlob("f🌟2".getBytes(StandardCharsets.UTF_8)), 5);
    rs.updateRow();
    assertArrayEquals(
        "f🌟".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));

    assertTrue(rs.next());
    assertNull(rs.getBlob(2));

    assertTrue(rs.next());
    assertArrayEquals(
        "e🌟3".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));
    assertTrue(rs.next());
    assertNull(rs.getBlob(2));
    assertTrue(rs.next());
    assertArrayEquals(
        "e🌟4".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));
    assertTrue(rs.next());
    assertArrayEquals(
        "e🌟5".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));
    assertTrue(rs.next());
    assertArrayEquals(fileContent, rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));
    assertTrue(rs.next());
    assertArrayEquals(
        Arrays.copyOfRange(fileContent, 0, 5000),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));
    assertTrue(rs.next());
    assertArrayEquals(fileContent, rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));
    assertTrue(rs.next());
    assertArrayEquals(
        Arrays.copyOfRange(fileContent, 0, 5000),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));
    assertTrue(rs.next());
    assertArrayEquals(fileContent, rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));
    assertTrue(rs.next());
    assertArrayEquals(
        Arrays.copyOfRange(fileContent, 0, 5000),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));
    assertTrue(rs.next());
    assertArrayEquals(
        "e🌟6''".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));
    assertTrue(rs.next());
    assertArrayEquals("e🌟7".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));
    assertTrue(rs.next());
    assertArrayEquals("e🌟8".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));
    rs.updateBytes("t1", "2g🌟12".getBytes(StandardCharsets.UTF_8));
    rs.updateRow();
    assertArrayEquals(
        "2g🌟12".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));
    assertTrue(rs.next());
    assertArrayEquals("e🌟9''\\n".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));
    rs.updateBytes(2, "2g🌟15".getBytes(StandardCharsets.UTF_8));
    rs.updateRow();
    assertArrayEquals(
        "2g🌟15".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));

    assertTrue(rs.next());
    assertArrayEquals("e🌟9".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));
    rs.updateBinaryStream(
        "t1", new ByteArrayInputStream("2g🌟15".getBytes(StandardCharsets.UTF_8)));
    rs.updateRow();
    assertArrayEquals("2g🌟15".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));

    assertTrue(rs.next());
    assertArrayEquals("e🌟1".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));
    rs.updateBinaryStream(
        "t1", new ByteArrayInputStream("2g🌟15".getBytes(StandardCharsets.UTF_8)), 7);
    rs.updateRow();
    assertArrayEquals("2g🌟1".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));

    assertTrue(rs.next());
    assertArrayEquals("e🌟4".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));
    rs.updateBinaryStream(
        "t1", new ByteArrayInputStream("2g🌟456".getBytes(StandardCharsets.UTF_8)), 7L);
    rs.updateRow();
    assertArrayEquals("2g🌟4".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));

    assertTrue(rs.next());
    assertArrayEquals("e🌟9".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));
    rs.updateBinaryStream(2, new ByteArrayInputStream("2g🌟15".getBytes(StandardCharsets.UTF_8)));
    rs.updateRow();
    assertArrayEquals("2g🌟15".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));

    assertTrue(rs.next());
    assertArrayEquals("e🌟1".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));
    rs.updateBinaryStream(
        2, new ByteArrayInputStream("2g🌟15".getBytes(StandardCharsets.UTF_8)), 7);
    rs.updateRow();
    assertArrayEquals("2g🌟1".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));

    assertTrue(rs.next());
    assertArrayEquals("e🌟4".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));
    rs.updateBinaryStream(
        2, new ByteArrayInputStream("2g🌟456".getBytes(StandardCharsets.UTF_8)), 7L);
    rs.updateRow();
    assertArrayEquals("2g🌟4".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));

    rs = stmt.executeQuery("SELECT * FROM BlobCodec2");
    assertTrue(rs.next());
    assertTrue(rs.next());
    assertNull(rs.getBlob(2));

    assertTrue(rs.next());
    Blob blob = rs.getBlob(2);
    assertArrayEquals(
        "g🌟1".getBytes(StandardCharsets.UTF_8), blob.getBytes(1, (int) blob.length()));

    assertTrue(rs.next());
    blob = rs.getBlob(2);
    assertArrayEquals(
        "f🌟1".getBytes(StandardCharsets.UTF_8), blob.getBytes(1, (int) blob.length()));

    assertTrue(rs.next());
    assertNull(rs.getBlob(2));

    assertTrue(rs.next());
    blob = rs.getBlob(2);
    assertArrayEquals(
        "f🌟".getBytes(StandardCharsets.UTF_8), blob.getBytes(1, (int) blob.length()));

    assertTrue(rs.next());
    assertNull(rs.getBlob(2));

    assertTrue(rs.next());
    blob = rs.getBlob(2);
    assertArrayEquals(
        "e🌟3".getBytes(StandardCharsets.UTF_8), blob.getBytes(1, (int) blob.length()));
    assertTrue(rs.next());
    assertNull(rs.getBlob(2));
    assertTrue(rs.next());
    blob = rs.getBlob(2);
    assertArrayEquals(
        "e🌟4".getBytes(StandardCharsets.UTF_8), blob.getBytes(1, (int) blob.length()));
    assertTrue(rs.next());
    blob = rs.getBlob(2);
    assertArrayEquals(
        "e🌟5".getBytes(StandardCharsets.UTF_8), blob.getBytes(1, (int) blob.length()));
    assertTrue(rs.next());
    blob = rs.getBlob(2);
    assertArrayEquals(fileContent, blob.getBytes(1, (int) blob.length()));
    assertTrue(rs.next());
    blob = rs.getBlob(2);
    assertArrayEquals(
        Arrays.copyOfRange(fileContent, 0, 5000), blob.getBytes(1, (int) blob.length()));
    assertTrue(rs.next());
    blob = rs.getBlob(2);
    assertArrayEquals(fileContent, blob.getBytes(1, (int) blob.length()));
    assertTrue(rs.next());
    blob = rs.getBlob(2);
    assertArrayEquals(
        Arrays.copyOfRange(fileContent, 0, 5000), blob.getBytes(1, (int) blob.length()));
    assertTrue(rs.next());
    blob = rs.getBlob(2);
    assertArrayEquals(fileContent, blob.getBytes(1, (int) blob.length()));
    assertTrue(rs.next());
    blob = rs.getBlob(2);
    assertArrayEquals(
        Arrays.copyOfRange(fileContent, 0, 5000), blob.getBytes(1, (int) blob.length()));
    assertTrue(rs.next());
    blob = rs.getBlob(2);
    assertArrayEquals(
        "e🌟6''".getBytes(StandardCharsets.UTF_8), blob.getBytes(1, (int) blob.length()));
    assertTrue(rs.next());
    assertArrayEquals("e🌟7".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));
    assertTrue(rs.next());
    blob = rs.getBlob(2);
    assertArrayEquals(
        "2g🌟12".getBytes(StandardCharsets.UTF_8), blob.getBytes(1, (int) blob.length()));
    assertTrue(rs.next());
    blob = rs.getBlob(2);
    assertArrayEquals(
        "2g🌟15".getBytes(StandardCharsets.UTF_8), blob.getBytes(1, (int) blob.length()));

    assertTrue(rs.next());
    assertArrayEquals("2g🌟15".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));

    assertTrue(rs.next());
    assertArrayEquals("2g🌟1".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));

    assertTrue(rs.next());
    assertArrayEquals("2g🌟4".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));

    assertTrue(rs.next());
    assertArrayEquals("2g🌟15".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));

    assertTrue(rs.next());
    assertArrayEquals("2g🌟1".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));

    assertTrue(rs.next());
    assertArrayEquals("2g🌟4".getBytes(StandardCharsets.UTF_8), rs.getBytes(2));
    con.commit();
  }

  private static class BlobInputStream implements Blob {
    private final InputStream data;

    public BlobInputStream(InputStream data) {
      this.data = data;
    }

    @Override
    public long length() throws SQLException {
      throw new SQLException("Length not available");
    }

    @Override
    public byte[] getBytes(final long pos, final int length) throws SQLException {

      // if not have thrown an error
      byte[] buf = new byte[length];
      int len;
      int intpos = 0;
      try {
        while ((len = data.read(buf, intpos, length - intpos)) > 0) {
          intpos += len;
          if (pos >= len) break;
        }
        return buf;
      } catch (IOException io) {
        throw new SQLException("Error reading stream");
      }
    }

    @Override
    public InputStream getBinaryStream() {
      return data;
    }

    @Override
    public long position(byte[] bytes, long l) {
      return 0;
    }

    @Override
    public long position(Blob blob, long l) {
      return 0;
    }

    @Override
    public int setBytes(long l, byte[] bytes) {
      return 0;
    }

    @Override
    public int setBytes(long l, byte[] bytes, int i, int i1) {
      return 0;
    }

    @Override
    public OutputStream setBinaryStream(long l) {
      return null;
    }

    @Override
    public void truncate(long l) {
      throw new UnsupportedOperationException("notImplemented()");
    }

    @Override
    public void free() {
      throw new UnsupportedOperationException("notImplemented()");
    }

    @Override
    public InputStream getBinaryStream(long l, long l1) {
      return null;
    }
  }
}
