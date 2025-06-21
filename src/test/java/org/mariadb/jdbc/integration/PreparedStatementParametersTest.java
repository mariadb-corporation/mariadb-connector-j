// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
    stmt.execute("CREATE TABLE prepareParam7 (t1 TIME(6))");
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
    stmt.execute("DROP TABLE IF EXISTS prepareParam7");
  }

  @Test
  public void validateParameters() throws Exception {
    // error crashing maxscale 6.1.x
    Assumptions.assumeTrue(
        !sharedConn.getMetaData().getDatabaseProductVersion().contains("maxScale-6.1."));
    validateParameters(sharedConn);
    validateParameters(sharedConnBinary);
  }

  public void validateParameters(Connection con) throws Exception {
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO prepareParam6 VALUES (?)")) {
      try {
        prep.execute();
        fail();
      } catch (SQLException sqle) {
        // eat
      }
    }
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO prepareParam6 VALUES (?)")) {
      prep.setInt(1, 1);
      prep.execute();
      prep.clearParameters();
      Common.assertThrowsContains(
          SQLTransientConnectionException.class,
          prep::execute,
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
                new BigDecimal("-156.59"), rs.getBigDecimal(1).setScale(2, RoundingMode.HALF_DOWN)),
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
    boolean minus = TimeZone.getDefault().getOffset(System.currentTimeMillis()) > 0;

    ZonedDateTime zdt =
        LocalDateTime.parse((minus ? "2010-01-11" : "2010-01-12") + "T00:00:00.0")
            .atZone(TimeZone.getTimeZone("UTC").toZoneId());

    checkSendTimestamp(
        ps -> ps.setDate(1, Date.valueOf("2010-01-12"), utcCal),
        rs -> assertEquals(zdt.toEpochSecond() * 1000, rs.getDate(1, utcCal).getTime()),
        con);
    checkSendTimestamp(
        ps -> ps.setDate(1, Date.valueOf("2010-05-25")),
        rs -> assertEquals(Date.valueOf("2010-05-25").getTime(), rs.getDate(1).getTime()),
        con);
    if (text) {
      Common.assertThrowsContains(
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
        "\uD83D\uDE0E" // 😎 unicode 6 smiling face with sunglasses
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
        ps -> ps.setURL(1, new URL("https://www.someUrl.com")),
        rs -> assertEquals("https://www.someUrl.com", rs.getString(1)),
        con);
    checkSendString(ps -> ps.setURL(1, (URL) null), rs -> assertNull(rs.getString(1)), con);
    // TODO SET OBJECT
  }

  @Test
  public void checkTimeParameters() throws Exception {
    checkTimeParameters(sharedConn);
    checkTimeParameters(sharedConnBinary);
  }

  @SuppressWarnings("deprecation")
  public void checkTimeParameters(Connection con) throws Exception {
    Calendar utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    checkSendTime(
        ps -> ps.setTime(1, new Time(Time.valueOf("18:16:01").getTime())),
        rs -> assertEquals("18:16:01", rs.getTime(1).toString()),
        con);
    checkSendTime(
        ps -> ps.setTime(1, new Time(Time.valueOf("18:16:01").getTime()), utcCal),
        rs -> assertEquals("18:16:01", rs.getTime(1, utcCal).toString()),
        con);
    checkSendTime(
        ps -> ps.setTime(1, new Time(Time.valueOf("18:16:01").getTime() + 123), utcCal),
        rs ->
            assertEquals(
                Time.valueOf("18:16:01").getTime() + 123 - TimeZone.getDefault().getOffset(0),
                rs.getTime(1).getTime()),
        con);
  }

  @Test
  public void checkNotSupported() throws Exception {
    checkNotSupported(sharedConn);
    checkNotSupported(sharedConnBinary);
  }

  private void checkNotSupported(Connection con) throws SQLException {
    try (PreparedStatement preparedStatement = con.prepareStatement("SELECT * FROM prepareParam")) {
      Common.assertThrowsContains(
          SQLException.class,
          () -> preparedStatement.setRef(1, null),
          "REF parameter are not supported");
      Common.assertThrowsContains(
          SQLException.class,
          () -> preparedStatement.setObject(1, "", Types.REF),
          "Type not supported");
      preparedStatement.setArray(1, null);
      Common.assertThrowsContains(
          SQLException.class,
          () -> con.createArrayOf("String", new Float[] {1f}),
          "typeName String is not supported");
      Common.assertThrowsContains(
          SQLException.class,
          () ->
              preparedStatement.setArray(
                  1,
                  new Array() {
                    @Override
                    public String getBaseTypeName() throws SQLException {
                      return null;
                    }

                    @Override
                    public int getBaseType() throws SQLException {
                      return 0;
                    }

                    @Override
                    public Object getArray() throws SQLException {
                      return null;
                    }

                    @Override
                    public Object getArray(Map<String, Class<?>> map) throws SQLException {
                      return null;
                    }

                    @Override
                    public Object getArray(long index, int count) throws SQLException {
                      return null;
                    }

                    @Override
                    public Object getArray(long index, int count, Map<String, Class<?>> map)
                        throws SQLException {
                      return null;
                    }

                    @Override
                    public ResultSet getResultSet() throws SQLException {
                      return null;
                    }

                    @Override
                    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
                      return null;
                    }

                    @Override
                    public ResultSet getResultSet(long index, int count) throws SQLException {
                      return null;
                    }

                    @Override
                    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
                        throws SQLException {
                      return null;
                    }

                    @Override
                    public void free() throws SQLException {}
                  }),
          "this type of Array parameter");
      Common.assertThrowsContains(
          SQLException.class,
          () -> preparedStatement.setRowId(1, null),
          "RowId parameter are not supported");
      Common.assertThrowsContains(
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

  private void checkSendTime(
      ThrowingConsumer<PreparedStatement, Exception> consumer,
      ThrowingConsumer<ResultSet, Exception> check,
      Connection con)
      throws Exception {
    checkSend(consumer, check, "prepareParam7", con);
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
        con.prepareStatement(String.format("INSERT INTO %s VALUES (?)", table))) {
      consumer.accept(preparedStatement);
      preparedStatement.execute();
    }
    ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s", table));
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
    Assumptions.assumeTrue(maxAllowedPacket < 32 * 1024 * 1024);
    char[] arr = new char[maxAllowedPacket];
    for (int pos = 0; pos < arr.length; pos++) {
      arr[pos] = (char) ('A' + (pos % 60));
    }
    boolean expectClosed = maxAllowedPacket >= 16 * 1024 * 1024;
    String st = new String(arr);
    try (Connection con = createCon("maxAllowedPacket=" + maxAllowedPacket)) {
      bigSendError(con, st, expectClosed);
    }
    try (Connection con =
        createCon("useServerPrepStmts=true&maxAllowedPacket=" + maxAllowedPacket)) {
      bigSendError(con, st, expectClosed);
    }
    try (Connection con = createCon("transactionReplay&maxAllowedPacket=" + maxAllowedPacket)) {
      bigSendError(con, st, expectClosed);
    }
  }

  public void bigSendError(Connection con, String st, boolean expectClose) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE bigTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO bigTest VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setString(2, st);
      Common.assertThrowsContains(
          SQLException.class,
          prep::execute,
          "Packet too big for current server max_allowed_packet value");
      assertEquals(expectClose, con.isClosed());
    }
    if (!con.isClosed()) con.commit();
  }

  @Test
  public void bigSendErrorMax() throws SQLException {
    Assumptions.assumeTrue(!isMaxscale());

    int maxAllowedPacket = getMaxAllowedPacket();
    Assumptions.assumeTrue(
        maxAllowedPacket > 16 * 1024 * 1024 && maxAllowedPacket < 100 * 1024 * 1024);
    char[] arr = new char[maxAllowedPacket + 100];
    for (int pos = 0; pos < arr.length; pos++) {
      arr[pos] = (char) ('A' + (pos % 60));
    }
    String st = new String(arr);
    try (Connection con = createCon("maxAllowedPacket=" + maxAllowedPacket)) {
      bigSendErrorMax(con, st, true);
    }
    try (Connection con =
        createCon("useServerPrepStmts=true&maxAllowedPacket=" + maxAllowedPacket)) {
      bigSendErrorMax(con, st, true);
    }
    try (Connection con = createCon("transactionReplay&maxAllowedPacket=" + maxAllowedPacket)) {
      bigSendError(con, st, true);
    }
  }

  public void bigSendErrorMax(Connection con, String st, boolean expectClose) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE bigTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO bigTest VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setString(2, st);
      Common.assertThrowsContains(
          SQLNonTransientConnectionException.class,
          prep::execute,
          "Packet too big for current server max_allowed_packet value");
      assertEquals(expectClose, con.isClosed());
    }
  }

  @FunctionalInterface
  public interface ThrowingConsumer<T, E extends Exception> {
    void accept(T t) throws E;
  }
}
