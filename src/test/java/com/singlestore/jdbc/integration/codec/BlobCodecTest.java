// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.MariaDbBlob;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.result.CompleteResult;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.*;
import java.util.Arrays;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class BlobCodecTest extends CommonCodecTest {
  private static File tmpFile;
  private static byte[] fileContent = new byte[11000];

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
    Statement stmt = sharedConn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from BlobCodec");
    assertTrue(rs.next());
    return rs;
  }

  private CompleteResult getPrepare(com.singlestore.jdbc.Connection con) throws SQLException {
    PreparedStatement stmt =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from BlobCodec"
                + " WHERE 1 > ?");
    stmt.closeOnCompletion();
    stmt.setInt(1, 0);
    CompleteResult rs = (CompleteResult) stmt.executeQuery();
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
  public void getString() throws SQLException {
    getString(get());
  }

  @Test
  public void getStringPrepare() throws SQLException {
    getString(getPrepare(sharedConn));
    getString(getPrepare(sharedConnBinary));
  }

  public void getString(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNString(1),
        "Data type BLOB cannot be decoded as String");
    assertFalse(rs.wasNull());
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNString(2),
        "Data type BLOB cannot be decoded as String");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNString("t2alias"),
        "Data type BLOB cannot be decoded as String");
    assertFalse(rs.wasNull());
    assertNull(rs.getNString(4));
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
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNString(1),
        "Data type BLOB cannot be decoded as String");
    assertFalse(rs.wasNull());
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNString(2),
        "Data type BLOB cannot be decoded as String");
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getNString("t2alias"),
        "Data type BLOB cannot be decoded as String");
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
    rs.next();
    rs.next();
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getByte(1),
        "empty String value cannot be decoded as Byte");
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
  public void getBigIntegerPrepare() throws SQLException {
    getBigInteger(getPrepare(sharedConn));
    getBigInteger(getPrepare(sharedConnBinary));
  }

  public void getBigInteger(CompleteResult rs) throws SQLException {
    assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBigInteger(1),
        "Data type BLOB cannot be decoded as BigInteger");
    assertFalse(rs.wasNull());
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

  public void getDuration(ResultSet rs) throws SQLException {
    assertThrowsContains(
        SQLException.class,
        () -> rs.getObject(1, Duration.class),
        "Data type BLOB cannot be decoded as Duration");
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
  public void sendParam() throws Exception {
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
    assertArrayEquals(
        "g🌟1".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));

    assertTrue(rs.next());
    assertArrayEquals(
        "f🌟1".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));

    assertTrue(rs.next());
    assertNull(rs.getBlob(2));

    assertTrue(rs.next());
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
    assertArrayEquals(
        "2g🌟12".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));
    assertTrue(rs.next());
    assertArrayEquals(
        "2g🌟15".getBytes(StandardCharsets.UTF_8),
        rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length()));

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
  }

  private class BlobInputStream implements Blob {
    private InputStream data;

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
      byte[] array = new byte[4096];
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
    public InputStream getBinaryStream() throws SQLException {
      return data;
    }

    @Override
    public long position(byte[] bytes, long l) throws SQLException {
      return 0;
    }

    @Override
    public long position(Blob blob, long l) throws SQLException {
      return 0;
    }

    @Override
    public int setBytes(long l, byte[] bytes) throws SQLException {
      return 0;
    }

    @Override
    public int setBytes(long l, byte[] bytes, int i, int i1) throws SQLException {
      return 0;
    }

    @Override
    public OutputStream setBinaryStream(long l) throws SQLException {
      return null;
    }

    @Override
    public void truncate(long l) throws SQLException {}

    @Override
    public void free() throws SQLException {}

    @Override
    public InputStream getBinaryStream(long l, long l1) throws SQLException {
      return null;
    }
  }
}
