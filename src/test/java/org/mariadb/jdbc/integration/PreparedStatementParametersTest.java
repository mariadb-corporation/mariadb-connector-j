// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.MariaDbClob;
import org.mariadb.jdbc.Statement;

public class PreparedStatementParametersTest extends Common {

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE bigTest (t1 int not null primary key auto_increment, t2 LONGTEXT)");
    stmt.execute("CREATE TABLE prepareParam (t1 BLOB(20))");
    stmt.execute("CREATE TABLE prepareParam2 (t1 BIGINT)");
    stmt.execute("CREATE TABLE prepareParam3 (t1 DOUBLE)");
    stmt.execute(
        "CREATE TABLE prepareParam4 (t1 VARCHAR(30)) "
            + "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
    stmt.execute("CREATE TABLE prepareParam5 (t1 TIMESTAMP(6))");
    stmt.execute("CREATE TABLE prepareParam6 (t1 BIGINT)");
    stmt.execute("FLUSH TABLES");
  }

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS bigTest");
    stmt.execute("DROP TABLE IF EXISTS prepareParam");
    stmt.execute("DROP TABLE IF EXISTS prepareParam2");
    stmt.execute("DROP TABLE IF EXISTS prepareParam3");
    stmt.execute("DROP TABLE IF EXISTS prepareParam4");
    stmt.execute("DROP TABLE IF EXISTS prepareParam5");
    stmt.execute("DROP TABLE IF EXISTS prepareParam6");
  }

  @Test
  public void validateParameters() throws Exception {
    validateParameters(sharedConn, true);
    validateParameters(sharedConnBinary, false);
  }

  public void validateParameters(org.mariadb.jdbc.Connection con, boolean text) throws Exception {
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO prepareParam6 VALUES (?)")) {
      try {
        prep.execute();
        fail();
      } catch (SQLException sqle) {
      }
    }
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO prepareParam6 VALUES (?)")) {
      prep.setInt(1, 1);
      prep.execute();
      prep.clearParameters();
      assertThrowsContains(
          SQLTransientConnectionException.class,
          () -> prep.execute(),
          "Parameter at position 1 is not set");
    }
  }

  @Test
  public void checkParameters() throws Exception {
    checkParameters(sharedConn, true);
    checkParameters(sharedConnBinary, false);
  }

  @SuppressWarnings("deprecation")
  public void checkParameters(org.mariadb.jdbc.Connection con, boolean text) throws Exception {
    Calendar utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    checkSendBlob(
        ps -> ps.setBlob(1, new MariaDbBlob("0123".getBytes(), 1, 2)),
        rs -> assertArrayEquals("12".getBytes(), rs.getBytes(1)),
        con);
    checkSendBlob(
        ps -> ps.setBlob(1, new MariaDbBlob("0123".getBytes(), 1, 2).getBinaryStream()),
        rs -> assertArrayEquals("12".getBytes(), rs.getBytes(1)),
        con);
    checkSendBlob(
        ps -> ps.setBlob(1, new MariaDbBlob("01234".getBytes(), 1, 3).getBinaryStream(), 2),
        rs -> assertArrayEquals("12".getBytes(), rs.getBytes(1)),
        con);
    checkSendBlob(
        ps -> ps.setBlob(1, new MariaDbBlob("01234".getBytes(), 1, 3).getBinaryStream(), 2L),
        rs -> assertArrayEquals("12".getBytes(), rs.getBytes(1)),
        con);
    checkSendBlob(ps -> ps.setNull(1, Types.VARBINARY), rs -> assertNull(rs.getObject(1)), con);
    checkSendBlob(
        ps -> ps.setNull(1, Types.VARBINARY, String.class.getName()),
        rs -> assertNull(rs.getObject(1)),
        con);
    checkSendLong(ps -> ps.setBoolean(1, true), rs -> assertTrue(rs.getBoolean(1)), con);
    checkSendLong(ps -> ps.setBoolean(1, false), rs -> assertFalse(rs.getBoolean(1)), con);
    checkSendLong(
        ps -> ps.setByte(1, (byte) 0x58), rs -> assertEquals((byte) 0x58, rs.getByte(1)), con);
    checkSendLong(
        ps -> ps.setShort(1, (short) 127), rs -> assertEquals((short) 127, rs.getShort(1)), con);
    checkSendLong(ps -> ps.setInt(1, -555), rs -> assertEquals(-555, rs.getInt(1)), con);
    checkSendLong(ps -> ps.setLong(1, -999L), rs -> assertEquals(-999L, rs.getLong(1)), con);
    checkSendDouble(
        ps -> ps.setFloat(1, -56.59F), rs -> assertEquals(-56.59F, rs.getFloat(1), 0.01F), con);
    checkSendDouble(
        ps -> ps.setDouble(1, -56.59D), rs -> assertEquals(-56.59D, rs.getDouble(1), 0.01D), con);
    checkSendDouble(
        ps -> ps.setBigDecimal(1, new BigDecimal("-156.59")),
        rs ->
            assertEquals(
                new BigDecimal("-156.59"),
                rs.getBigDecimal(1).setScale(2, BigDecimal.ROUND_HALF_DOWN)),
        con);
    checkSendString(
        ps -> ps.setString(1, "你好(hello in Chinese)"),
        rs -> assertEquals("你好(hello in Chinese)", rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setNString(1, "你好(hello in Chinese)"),
        rs -> assertEquals("你好(hello in Chinese)", rs.getNString(1)),
        con);
    checkSendBlob(
        ps -> ps.setBytes(1, "01234".getBytes()),
        rs -> assertArrayEquals("01234".getBytes(), rs.getBytes(1)),
        con);
    checkSendTimestamp(
        ps -> ps.setDate(1, Date.valueOf("2010-05-25")),
        rs -> assertEquals(Date.valueOf("2010-05-25"), rs.getDate(1)),
        con);
    checkSendTimestamp(
        ps -> ps.setDate(1, Date.valueOf("2010-01-12"), utcCal),
        rs -> assertEquals(1263250800000L, rs.getDate(1, utcCal).getTime()),
        con);
    checkSendTimestamp(
        ps -> ps.setDate(1, Date.valueOf("2010-01-12"), utcCal),
        rs -> assertEquals("2010-01-12", rs.getDate(1, utcCal).toString()),
        con);
    checkSendTimestamp(
        ps -> ps.setDate(1, Date.valueOf("2010-05-25")),
        rs -> assertEquals(Date.valueOf("2010-05-25").getTime(), rs.getDate(1).getTime()),
        con);
    if (text) {
      assertThrowsContains(
          SQLException.class,
          () ->
              checkSendTimestamp(
                  ps -> ps.setTime(1, new Time(Time.valueOf("18:16:01").getTime() + 123)),
                  rs ->
                      assertEquals(
                          Time.valueOf("18:16:01").getTime() + 123, rs.getTime(1).getTime()),
                  con),
          "Incorrect datetime value: '18:16:01.123'");
    } else {
      checkSendTimestamp(
          ps -> ps.setTime(1, new Time(Time.valueOf("18:16:01").getTime() + 123)),
          rs -> assertEquals(Time.valueOf("18:16:01").getTime() + 123, rs.getTime(1).getTime()),
          con);
      checkSendTimestamp(
          ps -> ps.setTime(1, new Time(Time.valueOf("18:16:01").getTime() + 123)),
          rs -> assertEquals("18:16:01", rs.getTime(1).toString()),
          con);
      checkSendTimestamp(
          ps -> ps.setTime(1, new Time(Time.valueOf("18:16:01").getTime() + 123), utcCal),
          rs -> assertEquals("18:16:01", rs.getTime(1, utcCal).toString()),
          con);
      checkSendTimestamp(
          ps -> ps.setTime(1, new Time(Time.valueOf("18:16:01").getTime() + 123), utcCal),
          rs ->
              assertEquals(
                  Time.valueOf("18:16:01").getTime() + 123 - TimeZone.getDefault().getDSTSavings(),
                  rs.getTime(1).getTime()),
          con);
    }
    checkSendTimestamp(
        ps -> ps.setTimestamp(1, Timestamp.valueOf("2010-05-25 18:16:01.987")),
        rs ->
            assertEquals(
                Timestamp.valueOf("2010-05-25 18:16:01.987").getTime(),
                rs.getTimestamp(1).getTime()),
        con);
    checkSendTimestamp(
        ps -> ps.setTimestamp(1, Timestamp.valueOf("2010-05-25 18:16:01.987")),
        rs -> assertEquals("2010-05-25 18:16:01.987", rs.getTimestamp(1).toString()),
        con);
    checkSendTimestamp(
        ps -> ps.setTimestamp(1, Timestamp.valueOf("2010-05-25 18:16:01.987"), utcCal),
        rs ->
            assertEquals(
                Timestamp.valueOf("2010-05-25 18:16:01.987").getTime(),
                rs.getTimestamp(1, utcCal).getTime()),
        con);

    checkSendTimestamp(
        ps -> ps.setTimestamp(1, Timestamp.valueOf("2010-05-25 18:16:01.987"), utcCal),
        rs ->
            assertEquals(
                Timestamp.valueOf("2010-05-25 18:16:01.987").getTime()
                    - TimeZone.getDefault().getOffset(1, 2010, 5, 25, 1, 987),
                rs.getTimestamp(1).getTime()),
        con);
    checkSendString(
        ps -> ps.setAsciiStream(1, new ByteArrayInputStream("abcdef".getBytes())),
        rs -> assertEquals("abcdef", rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setAsciiStream(1, new ByteArrayInputStream("abcdef".getBytes()), 5),
        rs -> assertEquals("abcde", rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setAsciiStream(1, new ByteArrayInputStream("abcdef".getBytes()), 5L),
        rs -> assertEquals("abcde", rs.getString(1)),
        con);
    final String unicodeString =
        ""
            + "\uD83D\uDE0E" // 😎 unicode 6 smiling face with sunglasses
            + "\uD83C\uDF36" // 🌶 unicode 7 hot pepper
            + "\uD83C\uDFA4" // 🎤 unicode 8 no microphones
            + "\uD83E\uDD42 "; // 🥂 unicode 9 clinking glasses
    final byte[] unicodeBytes = unicodeString.getBytes(StandardCharsets.UTF_8);
    checkSendString(
        ps -> ps.setUnicodeStream(1, new ByteArrayInputStream(unicodeBytes), 16),
        rs ->
            assertEquals(
                unicodeString.substring(0, 8),
                rs.getString(1),
                "expected " + unicodeString.substring(0, 8) + " but is " + rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setBinaryStream(1, new ByteArrayInputStream(unicodeBytes)),
        rs -> assertEquals(unicodeString, rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setBinaryStream(1, new ByteArrayInputStream(unicodeBytes), 16),
        rs -> assertEquals(unicodeString.substring(0, 8), rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setBinaryStream(1, new ByteArrayInputStream(unicodeBytes), 16L),
        rs -> assertEquals(unicodeString.substring(0, 8), rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setCharacterStream(1, new StringReader(unicodeString)),
        rs -> assertEquals(unicodeString, rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setCharacterStream(1, new StringReader(unicodeString), 8),
        rs -> assertEquals(unicodeString.substring(0, 8), rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setCharacterStream(1, new StringReader(unicodeString), 8L),
        rs -> assertEquals(unicodeString.substring(0, 8), rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setNCharacterStream(1, new StringReader(unicodeString)),
        rs -> assertEquals(unicodeString, rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setNCharacterStream(1, new StringReader(unicodeString), 8),
        rs -> assertEquals(unicodeString.substring(0, 8), rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setNCharacterStream(1, new StringReader(unicodeString), 8L),
        rs -> assertEquals(unicodeString.substring(0, 8), rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setClob(1, new MariaDbClob(unicodeBytes, 0, 16)),
        rs -> assertEquals(unicodeString.substring(0, 8), rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setClob(1, new StringReader(unicodeString)),
        rs -> assertEquals(unicodeString, rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setClob(1, new StringReader(unicodeString), 8),
        rs -> assertEquals(unicodeString.substring(0, 8), rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setClob(1, new StringReader(unicodeString), 8L),
        rs -> assertEquals(unicodeString.substring(0, 8), rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setNClob(1, new MariaDbClob(unicodeBytes, 0, 16)),
        rs -> assertEquals(unicodeString.substring(0, 8), rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setNClob(1, new StringReader(unicodeString)),
        rs -> assertEquals(unicodeString, rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setNClob(1, new StringReader(unicodeString), 8),
        rs -> assertEquals(unicodeString.substring(0, 8), rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setNClob(1, new StringReader(unicodeString), 8L),
        rs -> assertEquals(unicodeString.substring(0, 8), rs.getString(1)),
        con);
    checkSendString(
        ps -> ps.setURL(1, new URL("http://www.someUrl.com")),
        rs -> assertEquals("http://www.someUrl.com", rs.getString(1)),
        con);
    // TODO SET OBJECT
  }

  @Test
  public void checkNotSupported() throws Exception {
    checkNotSupported(sharedConn, true);
    checkNotSupported(sharedConnBinary, false);
  }

  private void checkNotSupported(Connection con, boolean text) throws SQLException {
    try (PreparedStatement preparedStatement = con.prepareStatement("SELECT * FROM prepareParam")) {
      assertThrowsContains(
          SQLException.class,
          () -> preparedStatement.setRef(1, null),
          "REF parameter are not supported");
      assertThrowsContains(
          SQLException.class,
          () -> preparedStatement.setArray(1, null),
          "Array parameter are not supported");
      assertThrowsContains(
          SQLException.class,
          () -> preparedStatement.setRowId(1, null),
          "RowId parameter are not supported");
      assertThrowsContains(
          SQLException.class,
          () -> preparedStatement.setSQLXML(1, null),
          "SQLXML parameter are not supported");
    }
  }

  private void checkSendBlob(
      ThrowingConsumer<PreparedStatement, Exception> consumer,
      ThrowingConsumer<ResultSet, Exception> check,
      Connection con)
      throws Exception {
    checkSend(consumer, check, "prepareParam", con);
  }

  private void checkSendLong(
      ThrowingConsumer<PreparedStatement, Exception> consumer,
      ThrowingConsumer<ResultSet, Exception> check,
      Connection con)
      throws Exception {
    checkSend(consumer, check, "prepareParam2", con);
  }

  private void checkSendDouble(
      ThrowingConsumer<PreparedStatement, Exception> consumer,
      ThrowingConsumer<ResultSet, Exception> check,
      Connection con)
      throws Exception {
    checkSend(consumer, check, "prepareParam3", con);
  }

  private void checkSendString(
      ThrowingConsumer<PreparedStatement, Exception> consumer,
      ThrowingConsumer<ResultSet, Exception> check,
      Connection con)
      throws Exception {
    checkSend(consumer, check, "prepareParam4", con);
  }

  private void checkSendTimestamp(
      ThrowingConsumer<PreparedStatement, Exception> consumer,
      ThrowingConsumer<ResultSet, Exception> check,
      Connection con)
      throws Exception {
    checkSend(consumer, check, "prepareParam5", con);
  }

  private void checkSend(
      ThrowingConsumer<PreparedStatement, Exception> consumer,
      ThrowingConsumer<ResultSet, Exception> check,
      String table,
      Connection con)
      throws Exception {
    Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION");
    try (PreparedStatement preparedStatement =
        con.prepareStatement("INSERT INTO " + table + " VALUES (?)")) {
      consumer.accept(preparedStatement);
      preparedStatement.execute();
    }
    ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);
    assertTrue(rs.next());
    con.rollback();
    check.accept(rs);
  }

  @Test
  public void bigSend() throws SQLException {
    int maxAllowedPacket = getMaxAllowedPacket();
    Assumptions.assumeTrue(maxAllowedPacket > 21 * 1024 * 1024);
    char[] arr = new char[20 * 1024 * 1024];
    for (int pos = 0; pos < arr.length; pos++) {
      arr[pos] = (char) ('A' + (pos % 60));
    }
    String st = new String(arr);
    bigSend(sharedConn, st);
    bigSend(sharedConnBinary, st);
    try (Connection con = createCon("useEof=false")) {
      bigSend(con, st);
    }
  }

  public void bigSend(Connection con, String st) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE bigTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO bigTest VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setString(2, st);
      prep.execute();
    }

    ResultSet rs = stmt.executeQuery("SELECT t2 from bigTest WHERE t1 = 1");
    assertTrue(rs.next());
    assertEquals(st, rs.getString(1));
    con.commit();
  }

  @Test
  public void bigSendError() throws SQLException {
    int maxAllowedPacket = getMaxAllowedPacket();
    Assumptions.assumeTrue(maxAllowedPacket < 10 * 1024 * 1024);
    char[] arr = new char[10 * 1024 * 1024];
    for (int pos = 0; pos < arr.length; pos++) {
      arr[pos] = (char) ('A' + (pos % 60));
    }
    String st = new String(arr);
    bigSendError(sharedConn, st);
    bigSendError(sharedConnBinary, st);
  }

  public void bigSendError(Connection con, String st) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE bigTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO bigTest VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setString(2, st);
      assertThrowsContains(
          SQLException.class,
          () -> prep.execute(),
          "Packet too big for current server max_allowed_packet value");
      assertFalse(con.isClosed());
    }
    con.commit();
  }

  @Test
  public void bigSendErrorMax() throws SQLException {
    Assumptions.assumeTrue(
        !"maxscale".equals(System.getenv("srv"))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));

    int maxAllowedPacket = getMaxAllowedPacket();
    Assumptions.assumeTrue(
        maxAllowedPacket > 16 * 1024 * 1024 && maxAllowedPacket < 100 * 1024 * 1024);
    char[] arr = new char[maxAllowedPacket + 100];
    for (int pos = 0; pos < arr.length; pos++) {
      arr[pos] = (char) ('A' + (pos % 60));
    }
    String st = new String(arr);
    try (Connection con = createCon()) {
      bigSendErrorMax(con, st);
    }
    try (Connection con = createCon("useServerPrepStmts=true")) {
      bigSendErrorMax(con, st);
    }
  }

  public void bigSendErrorMax(Connection con, String st) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE bigTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO bigTest VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setString(2, st);
      assertThrowsContains(
          SQLNonTransientConnectionException.class,
          () -> prep.execute(),
          "Packet too big for current server max_allowed_packet value");
      assertTrue(con.isClosed());
    }
  }

  @FunctionalInterface
  public interface ThrowingConsumer<T, E extends Exception> {
    void accept(T t) throws E;
  }
}
