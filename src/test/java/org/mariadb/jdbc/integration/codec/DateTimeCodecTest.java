// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.result.CompleteResult;
import org.mariadb.jdbc.integration.Common;

public class DateTimeCodecTest extends CommonCodecTest {
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS DateTimeCodec");
    stmt.execute("DROP TABLE IF EXISTS DateTimeCodec2");
    stmt.execute("DROP TABLE IF EXISTS DateTimeCodec3");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();

    // ensure not setting NO_ZERO_DATE and NO_ZERO_IN_DATE
    stmt.execute(
        "set sql_mode='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'");

    stmt.execute(
        "CREATE TABLE DateTimeCodec (t1 DATETIME , t2 DATETIME(6), t3 DATETIME(6), t4"
            + " DATETIME(6))");
    stmt.execute(
        "INSERT INTO DateTimeCodec VALUES ('2010-01-12 01:55:12', '1000-01-01 01:55:13.212345',"
            + " '9999-12-31 18:30:12.55', null)"
            + (isMariaDBServer()
                ? ",('0000-00-00 00:00:00', '0000-00-00 00:00:00', '9999-12-31 00:00:00.00', null)"
                    + ",('1980-00-10 00:00:00', '1970-10-00 00:00:00.0123', null, null)"
                : ""));
    stmt.execute(
        "CREATE TABLE DateTimeCodec2 (id int not null primary key auto_increment, t1 DATETIME(6))");
    stmt.execute(
        "CREATE TABLE DateTimeCodec3 (id int not null primary key auto_increment, t1 DATETIME(6))");

    stmt.execute("FLUSH TABLES");
  }

  private ResultSet get() throws SQLException {
    Statement stmt =
        sharedConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from DateTimeCodec");
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private ResultSet getPrepare(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    PreparedStatement prepStmt =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from DateTimeCodec"
                + " WHERE 1 > ?",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
    prepStmt.closeOnCompletion();
    prepStmt.setInt(1, 0);
    ResultSet rs = prepStmt.executeQuery();
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

  @Test
  public void getObjectPrepareTimeZone() throws SQLException {

    int hourOffset =
        TimeZone.getDefault()
                .getOffset(
                    LocalDateTime.parse("2010-01-12T01:55:12")
                            .atZone(TimeZone.getDefault().toZoneId())
                            .toEpochSecond()
                        * 1000)
            / 3600000;
    Assumptions.assumeTrue(hourOffset != 0);

    Statement stmt = sharedConn.createStatement();
    ResultSet rs1 = stmt.executeQuery("SELECT @@global.time_zone");
    rs1.next();
    String srvTz = rs1.getString(1);
    String currOffset =
        (hourOffset < 0 ? "-" : "+")
            + (Math.abs(hourOffset) < 10 ? "0" : "")
            + Math.abs(hourOffset)
            + ":00";
    try {
      setTz(stmt, hourOffset, 1);
      checkTz("2010-01-12 00:55:12", "2010-01-12", "00:55:12", false, currOffset, hourOffset);
      checkTz("2010-01-12 00:55:12", "2010-01-12", "00:55:12", true, currOffset, hourOffset);

      setTz(stmt, hourOffset, 2);
      checkTz("2010-01-11 23:55:12", "2010-01-11", "23:55:12", false, currOffset, hourOffset);
      checkTz("2010-01-11 23:55:12", "2010-01-11", "23:55:12", true, currOffset, hourOffset);

    } finally {
      stmt.execute("SET @@global.time_zone='" + srvTz + "'");
    }
  }

  private void checkTz(
      String expectedTimestamp,
      String expectedDate,
      String expectedTime,
      boolean usePrepare,
      String srvTz,
      int hourOffset)
      throws SQLException {
    Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    try (Connection conn =
        createCon(
            "connectionTimeZone=SERVER&forceConnectionTimeZoneToSession=true&preserveInstants=true&useServerPrepStmts="
                + usePrepare)) {
      ResultSet rs = getPrepare(conn);
      assertEquals(Timestamp.valueOf(expectedTimestamp).getTime(), rs.getTimestamp(1).getTime());
      assertEquals(expectedTimestamp + ".0", rs.getTimestamp(1).toString());
      assertEquals(Timestamp.valueOf(expectedTimestamp).getTime(), rs.getTimestamp(1).getTime());
      assertEquals(expectedTimestamp, rs.getString(1));
      assertEquals(
          ZonedDateTime.of(
                      LocalDateTime.parse(expectedDate + "T" + expectedTime),
                      ZoneId.systemDefault())
                  .toEpochSecond()
              * 1000,
          rs.getDate(1).getTime());
      assertEquals(expectedTime, rs.getTime(1).toString());

      ZonedDateTime dateUtc =
          LocalDateTime.parse("2010-01-12T01:55:12")
              .atZone(TimeZone.getTimeZone("UTC").toZoneId())
              .withZoneSameInstant(TimeZone.getDefault().toZoneId());

      assertEquals(dateUtc.toLocalTime().toString(), rs.getTime(1, utcCalendar).toString());
      assertEquals(
          dateUtc.toLocalDateTime().toString().replace("T", " ") + ".0",
          rs.getTimestamp(1, utcCalendar).toString());

      assertEquals(
          expectedTimestamp.replace(" ", "T") + srvTz,
          rs.getObject(1, OffsetDateTime.class).toString());
      String sdt = rs.getObject(1, ZonedDateTime.class).toString();
      assertEquals(expectedTimestamp.replace(" ", "T") + srvTz, sdt.substring(0, sdt.indexOf('[')));
      assertEquals(
          expectedTimestamp.replace(" ", "T"), rs.getObject(1, LocalDateTime.class).toString());
    }
    try (Connection conn =
        createCon(
            "connectionTimeZone=SERVER&forceConnectionTimeZoneToSession=true&preserveInstants=false&useServerPrepStmts="
                + usePrepare)) {
      ResultSet rs = getPrepare(conn);
      assertEquals(
          Timestamp.valueOf("2010-01-12 01:55:12").getTime(),
          ((Timestamp) rs.getObject(1)).getTime());
      assertEquals("2010-01-12 01:55:12", rs.getString(1));
      assertEquals("2010-01-12", rs.getDate(1).toString());
      assertEquals("01:55:12", rs.getTime(1).toString());
    }
  }

  private String setTz(Statement stmt, int hourOffset, int offset) throws SQLException {
    hourOffset += offset;
    String newTz =
        (hourOffset < 0 ? "-" : "+")
            + (Math.abs(hourOffset) < 10 ? "0" : "")
            + Math.abs(hourOffset)
            + ":00";
    stmt.execute("SET @@global.time_zone='" + newTz + "'");
    return newTz;
  }

  public void getObject(ResultSet rs) throws SQLException {
    assertFalse(rs.wasNull());
    assertEquals(
        Timestamp.valueOf("2010-01-12 01:55:12").getTime(),
        ((Timestamp) rs.getObject(1)).getTime());
    assertFalse(rs.wasNull());
    assertEquals(
        Timestamp.valueOf("1000-01-01 01:55:13.212345").getTime(),
        ((Timestamp) rs.getObject(2)).getTime());
    assertFalse(rs.wasNull());
    assertEquals(
        Timestamp.valueOf("9999-12-31 18:30:12.55").getTime(),
        ((Timestamp) rs.getObject(3)).getTime());
    assertFalse(rs.wasNull());
    assertNull(rs.getDate(4));
    assertTrue(rs.wasNull());
    if (isMariaDBServer()) {
      assertTrue(rs.next());
      assertNull(rs.getObject(1));
      assertTrue(rs.wasNull());
      assertNull(rs.getObject(1, LocalTime.class));
      assertTrue(rs.wasNull());
      rs.next();

      assertEquals(
          Timestamp.valueOf("1979-12-10 00:00:00").getTime(),
          ((Timestamp) rs.getObject(1)).getTime());
      assertEquals(
          Timestamp.valueOf("1970-09-30 00:00:00.012300").getTime(),
          ((Timestamp) rs.getObject(2)).getTime());
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
    testObject(rs, String.class, "2010-01-12 01:55:12");
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
    testObject(rs, LocalDateTime.class, LocalDateTime.parse("2010-01-12T01:55:12"));
    // get OffsetDateTime for "2010-01-12T01:55:12" corresponding with current zone id:
    OffsetDateTime expOffsetDateTime =
        OffsetDateTime.ofInstant(
            Timestamp.valueOf("2010-01-12 01:55:12").toInstant(), ZoneId.systemDefault());
    testObject(rs, OffsetDateTime.class, expOffsetDateTime);
    testObject(
        rs,
        Instant.class,
        ZonedDateTime.of(LocalDateTime.parse("2010-01-12T01:55:12"), ZoneId.systemDefault())
            .toInstant());
    testObject(rs, LocalTime.class, LocalTime.parse("01:55:12"));
    testObject(rs, Time.class, Time.valueOf("01:55:12"));
    testObject(rs, Timestamp.class, Timestamp.valueOf("2010-01-12 01:55:12"));
    testObject(
        rs,
        ZonedDateTime.class,
        LocalDateTime.parse("2010-01-12T01:55:12").atZone(ZoneId.systemDefault()));
    testObject(
        rs,
        java.util.Date.class,
        new Date(
            ZonedDateTime.of(
                        LocalDateTime.parse("2010-01-12T01:55:12.0"),
                        TimeZone.getDefault().toZoneId())
                    .toEpochSecond()
                * 1000));
  }

  @Test
  public void getString() throws SQLException {
    getString(get(), true, false);
  }

  @Test
  public void getStringPrepare() throws SQLException {
    getString(getPrepare(sharedConn), true, false);
    getString(getPrepare(sharedConnBinary), false, false);
    try (Connection con = createCon("&oldModeNoPrecisionTimestamp=true")) {
      getString(getPrepare(con), true, true);
    }
    try (Connection con = createCon("&oldModeNoPrecisionTimestamp=true&useServerPrepStmts=true")) {
      getString(getPrepare(con), false, true);
    }
  }

  public void getString(ResultSet rs, boolean text, boolean oldModeNoPrecisionTimestamp)
      throws SQLException {
    assertEquals(
        "2010-01-12 01:55:12" + (oldModeNoPrecisionTimestamp ? ".0" : ""), rs.getString(1));
    assertFalse(rs.wasNull());
    assertEquals("1000-01-01 01:55:13.212345", rs.getString(2));
    assertEquals("1000-01-01 01:55:13.212345", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertEquals(
        "9999-12-31 18:30:12.55" + (oldModeNoPrecisionTimestamp ? "" : "0000"), rs.getString(3));
    assertFalse(rs.wasNull());
    assertNull(rs.getString(4));
    assertTrue(rs.wasNull());
    if (isMariaDBServer()) {
      rs.next();
      assertEquals("0000-00-00 00:00:00", rs.getString(1));
      assertEquals("0000-00-00 00:00:00.000000", rs.getString(2));
      assertEquals(
          "9999-12-31 00:00:00.0" + (oldModeNoPrecisionTimestamp ? "" : "00000"), rs.getString(3));
      rs.next();
      assertEquals("1980-00-10 00:00:00", rs.getString(1));
      assertEquals("1970-10-00 00:00:00.012300", rs.getString(2));
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
    assertEquals("2010-01-12 01:55:12", rs.getNString(1));
    assertFalse(rs.wasNull());
    String s = rs.getNString(2);
    assertEquals("1000-01-01 01:55:13.212345", s);
    s = rs.getNString("t2alias");
    assertEquals("1000-01-01 01:55:13.212345", s);
    assertFalse(rs.wasNull());
    s = rs.getNString(3);
    assertTrue(s.equals("9999-12-31 18:30:12.550000") || s.equals("9999-12-31 18:30:12.550"));
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
        SQLException.class,
        () -> rs.getBoolean(1),
        "Data type DATETIME cannot be decoded as Boolean");
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
        SQLException.class, () -> rs.getByte(1), "Data type DATETIME cannot be decoded as Byte");
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
        SQLException.class, () -> rs.getShort(1), "Data type DATETIME cannot be decoded as Short");
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
        SQLException.class, () -> rs.getInt(1), "Data type DATETIME cannot be decoded as Integer");
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
        SQLException.class, () -> rs.getLong(1), "Data type DATETIME cannot be decoded as Long");
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
        SQLException.class, () -> rs.getFloat(1), "Data type DATETIME cannot be decoded as Float");
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
        SQLException.class,
        () -> rs.getDouble(1),
        "Data type DATETIME cannot be decoded as Double");
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
        "Data type DATETIME cannot be decoded as BigDecimal");
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
        ZonedDateTime.of(LocalDateTime.parse("2010-01-12T01:55:12.0"), ZoneId.of("UTC"))
                .toEpochSecond()
            * 1000,
        rs.getDate(1, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(
        ZonedDateTime.of(
                    LocalDateTime.parse("2010-01-12T01:55:12.0"), TimeZone.getDefault().toZoneId())
                .toEpochSecond()
            * 1000,
        rs.getDate(1).getTime());
    assertFalse(rs.wasNull());

    assertEquals(
        "1000-01-01", rs.getDate(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).toString());
    assertFalse(rs.wasNull());
    assertEquals(Timestamp.valueOf("1000-01-01 01:55:13.21234").getTime(), rs.getDate(2).getTime());
    assertFalse(rs.wasNull());
    assertEquals(
        ZonedDateTime.of(
                        LocalDateTime.parse("9999-12-31T18:30:12.55"),
                        TimeZone.getDefault().toZoneId())
                    .toEpochSecond()
                * 1000
            + 550,
        rs.getDate(3).getTime());
    assertFalse(rs.wasNull());
    assertNull(rs.getDate(4));
    assertTrue(rs.wasNull());
    if (isMariaDBServer()) {
      rs.next();
      assertNull(rs.getTime(1));
      assertNull(rs.getTime(2));
      assertNull(rs.getDate(1));
      assertTrue(rs.wasNull());
      assertNull(rs.getDate(2));
      assertTrue(rs.wasNull());
      assertNull(rs.getTimestamp(1));
      assertTrue(rs.wasNull());
      assertNull(rs.getObject(1));
      assertTrue(rs.wasNull());
      assertNull(rs.getObject(1, LocalDateTime.class));
      assertTrue(rs.wasNull());
      assertNull(rs.getObject(1, ZonedDateTime.class));
      assertTrue(rs.wasNull());
      assertNull(rs.getObject(1, LocalDate.class));
      assertTrue(rs.wasNull());
      assertNull(rs.getObject(1, LocalTime.class));
      assertTrue(rs.wasNull());
    }
  }

  @Test
  public void getDateTimezoneTest() throws SQLException {
    TimeZone initialTz = Calendar.getInstance().getTimeZone();

    TimeZone.setDefault(TimeZone.getTimeZone("GMT+11"));
    try (Connection con = createCon()) {
      // expect server tz to be different.
      ResultSet rs = con.createStatement().executeQuery("SELECT @@session.time_zone");
      rs.next();
      String zoneId = rs.getString(1);
      TimeZone serverTz = null;
      try {
        serverTz = TimeZone.getTimeZone(ZoneId.of(zoneId).normalized());
      } catch (DateTimeException e) {
        try {
          serverTz = TimeZone.getTimeZone(ZoneId.of(zoneId, ZoneId.SHORT_IDS).normalized());
        } catch (DateTimeException e2) {
          // unknown zone id
        }
      }
      assertNotEquals(TimeZone.getDefault(), serverTz);
    }

    TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
    try (Connection conGmt8 = createCon("timezone=auto")) {
      getDateTimezoneTestGmt8(conGmt8, getPrepare(conGmt8), TimeZone.getTimeZone("GMT+8"));
      try (Connection conGmt8Preserve = createCon("timezone=auto&preserveInstants=true")) {
        getDateTimezoneTestGmt8preserveInstants(
            conGmt8Preserve, getPrepare(conGmt8Preserve), TimeZone.getTimeZone("GMT+8"));
      }

      TimeZone.setDefault(TimeZone.getTimeZone("GMT-8"));
      try (Connection conGmtm8 = createCon("timezone=auto")) {
        getDateTimezoneTestGmtm8(conGmtm8, getPrepare(conGmtm8), TimeZone.getTimeZone("GMT-8"));
      }
      try (Connection conGmtm8 = createCon("timezone=auto&useServerPrepStmts=true")) {
        getDateTimezoneTestGmtm8(conGmtm8, getPrepare(conGmtm8), TimeZone.getTimeZone("GMT-8"));
      }
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      try (Connection conUtc = createCon("timezone=UTC")) {
        getDateTimezoneTestUtc(conUtc, getPrepare(conUtc), TimeZone.getTimeZone("UTC"));
      }

      TimeZone.setDefault(initialTz);
      try (Connection conAuto = createCon("timezone=auto")) {
        getDateTimezoneTestNormal(conAuto, getPrepare(conAuto));
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Setting configured timezone 'auto' fail on server"));
        assertTrue(e.getCause().getMessage().contains("Unknown or incorrect time zone:"));
      }
    } finally {
      TimeZone.setDefault(initialTz);
    }
  }

  public void getDateTimezoneTestGmt8(Connection conGmt8, ResultSet rs, TimeZone tz)
      throws SQLException {

    assertEquals("2010-01-12T01:55:12+08:00", rs.getObject(1, OffsetDateTime.class).toString());

    conGmt8.createStatement().execute("TRUNCATE TABLE DateTimeCodec3");
    try (PreparedStatement prep =
        conGmt8.prepareStatement("INSERT INTO DateTimeCodec3 values (?,?)")) {
      prep.setInt(1, -2);
      prep.setString(2, "2010-01-12 01:55:12");
      prep.execute();

      prep.setInt(1, 1);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T01:55:12+08:00"));
      prep.execute();

      prep.setInt(1, 2);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T01:55:12+01:00"));
      prep.execute();

      prep.setInt(1, 3);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T01:55:12Z"));
      prep.execute();

      prep.setInt(1, 4);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T17:55:12-04:00"));
      prep.execute();

      prep.setInt(1, 5);
      prep.setObject(2, Instant.parse("2010-01-12T17:55:13.152Z"));
      prep.execute();
    }
    conGmt8.commit();

    java.sql.Statement stmt = conGmt8.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prepStmt =
        conGmt8.prepareStatement(
            "select * from DateTimeCodec3",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)) {
      rs = prepStmt.executeQuery();
      rs.next();
      assertEquals("2010-01-12T01:55:12+08:00", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 01:55:12.000000", rs.getString(2));

      rs.next();
      assertEquals("2010-01-12T01:55:12+08:00", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 01:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263232512000L, rs.getTimestamp(2).getTime());
      assertEquals(
          "2010-01-12 09:55:12.0",
          rs.getTimestamp(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).toString());
      assertEquals("2010-01-12 01:55:12.000000", rs.getString(2));
      assertEquals("2010-01-12", rs.getDate(2).toString());
      assertEquals(
          "2010-01-12",
          rs.getDate(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).toString());
      assertEquals("2010-01-12T01:55:12", rs.getObject(2, LocalDateTime.class).toString());

      rs.next();
      assertEquals("2010-01-12T08:55:12+08:00", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 08:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263257712000L, rs.getTimestamp(2).getTime());
      assertEquals("2010-01-12 08:55:12.000000", rs.getString(2));
      assertEquals("2010-01-12", rs.getDate(2).toString());

      rs.next();
      assertEquals("2010-01-12T09:55:12+08:00", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 09:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263261312000L, rs.getTimestamp(2).getTime());
      assertEquals("2010-01-12 09:55:12.000000", rs.getString(2));
      assertEquals("2010-01-12", rs.getDate(2).toString());

      rs.next();
      assertEquals("2010-01-13T05:55:12+08:00", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-13 05:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263333312000L, rs.getTimestamp(2).getTime());
      assertEquals("2010-01-13 05:55:12.000000", rs.getString(2));
      assertEquals("2010-01-13", rs.getDate(2).toString());

      rs.next();
      assertEquals("2010-01-12T17:55:13.152Z", rs.getObject(2, Instant.class).toString());
    }
    conGmt8.rollback();
  }

  public void getDateTimezoneTestGmt8preserveInstants(Connection conGmt8, ResultSet rs, TimeZone tz)
      throws SQLException {

    assertEquals("2010-01-12T01:55:12+08:00", rs.getObject(1, OffsetDateTime.class).toString());

    conGmt8.createStatement().execute("TRUNCATE TABLE DateTimeCodec3");
    try (PreparedStatement prep =
        conGmt8.prepareStatement("INSERT INTO DateTimeCodec3 values (?,?)")) {
      prep.setInt(1, -2);
      prep.setString(2, "2010-01-12 01:55:12");
      prep.execute();

      prep.setInt(1, 1);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T01:55:12+08:00"));
      prep.execute();

      prep.setInt(1, 2);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T01:55:12+01:00"));
      prep.execute();

      prep.setInt(1, 3);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T01:55:12Z"));
      prep.execute();

      prep.setInt(1, 4);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T17:55:12-04:00"));
      prep.execute();

      prep.setInt(1, 5);
      prep.setObject(2, Instant.parse("2010-01-12T17:55:13.152Z"));
      prep.execute();
    }
    conGmt8.commit();

    java.sql.Statement stmt = conGmt8.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prepStmt =
        conGmt8.prepareStatement(
            "select * from DateTimeCodec3",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)) {
      rs = prepStmt.executeQuery();
      rs.next();
      assertEquals("2010-01-12T01:55:12+08:00", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 01:55:12.000000", rs.getString(2));

      rs.next();
      assertEquals("2010-01-12T01:55:12+08:00", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 01:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263232512000L, rs.getTimestamp(2).getTime());
      assertEquals(
          "2010-01-12 09:55:12.0",
          rs.getTimestamp(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).toString());
      assertEquals("2010-01-12 01:55:12.000000", rs.getString(2));
      assertEquals("2010-01-12", rs.getDate(2).toString());
      assertEquals(
          "2010-01-12",
          rs.getDate(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).toString());
      assertEquals("2010-01-12T01:55:12", rs.getObject(2, LocalDateTime.class).toString());

      rs.next();
      assertEquals("2010-01-12T08:55:12+08:00", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 08:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263257712000L, rs.getTimestamp(2).getTime());
      assertEquals("2010-01-12 08:55:12.000000", rs.getString(2));
      assertEquals("2010-01-12", rs.getDate(2).toString());

      rs.next();
      assertEquals("2010-01-12T09:55:12+08:00", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 09:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263261312000L, rs.getTimestamp(2).getTime());
      assertEquals("2010-01-12 09:55:12.000000", rs.getString(2));
      assertEquals("2010-01-12", rs.getDate(2).toString());

      rs.next();
      assertEquals("2010-01-13T05:55:12+08:00", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-13 05:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263333312000L, rs.getTimestamp(2).getTime());
      assertEquals("2010-01-13 05:55:12.000000", rs.getString(2));
      assertEquals("2010-01-13", rs.getDate(2).toString());

      rs.next();
      assertEquals("2010-01-12T17:55:13.152Z", rs.getObject(2, Instant.class).toString());
    }
    conGmt8.rollback();
  }

  public void getDateTimezoneTestGmtm8(Connection conGmt8, ResultSet rs, TimeZone tz)
      throws SQLException {

    assertEquals("2010-01-12T01:55:12-08:00", rs.getObject(1, OffsetDateTime.class).toString());

    conGmt8.createStatement().execute("TRUNCATE TABLE DateTimeCodec3");
    try (PreparedStatement prep =
        conGmt8.prepareStatement("INSERT INTO DateTimeCodec3 values (?,?)")) {
      prep.setInt(1, -2);
      prep.setString(2, "2010-01-12 01:55:12");
      prep.execute();

      prep.setInt(1, 1);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T01:55:12-08:00"));
      prep.execute();

      prep.setInt(1, 2);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T01:55:12-01:00"));
      prep.execute();

      prep.setInt(1, 3);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T01:55:12Z"));
      prep.execute();

      prep.setInt(1, 4);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T17:55:12+04:00"));
      prep.execute();

      prep.setInt(1, 5);
      prep.setObject(2, Instant.parse("2010-01-12T17:55:13.152Z"));
      prep.execute();
    }
    conGmt8.commit();

    java.sql.Statement stmt = conGmt8.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prepStmt =
        conGmt8.prepareStatement(
            "select * from DateTimeCodec3",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)) {
      rs = prepStmt.executeQuery();
      rs.next();
      assertEquals("2010-01-12T01:55:12-08:00", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 01:55:12.000000", rs.getString(2));

      rs.next();
      assertEquals("2010-01-12T01:55:12-08:00", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 01:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263290112000L, rs.getTimestamp(2).getTime());
      assertEquals(
          "2010-01-11 17:55:12.0",
          rs.getTimestamp(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).toString());
      assertEquals("2010-01-12 01:55:12.000000", rs.getString(2));
      assertEquals("2010-01-12", rs.getDate(2).toString());
      assertEquals(
          "2010-01-11",
          rs.getDate(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).toString());
      assertEquals("2010-01-12T01:55:12", rs.getObject(2, LocalDateTime.class).toString());

      rs.next();
      assertEquals("2010-01-11T18:55:12-08:00", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-11 18:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263264912000L, rs.getTimestamp(2).getTime());
      assertEquals("2010-01-11 18:55:12.000000", rs.getString(2));
      assertEquals("2010-01-11", rs.getDate(2).toString());

      rs.next();
      assertEquals("2010-01-11T17:55:12-08:00", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-11 17:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263261312000L, rs.getTimestamp(2).getTime());
      assertEquals("2010-01-11 17:55:12.000000", rs.getString(2));
      assertEquals("2010-01-11", rs.getDate(2).toString());

      rs.next();
      assertEquals("2010-01-12T05:55:12-08:00", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 05:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263304512000L, rs.getTimestamp(2).getTime());
      assertEquals("2010-01-12 05:55:12.000000", rs.getString(2));
      assertEquals("2010-01-12", rs.getDate(2).toString());

      rs.next();
      assertEquals("2010-01-12T17:55:13.152Z", rs.getObject(2, Instant.class).toString());
    }
    conGmt8.rollback();
  }

  public void getDateTimezoneTestUtc(Connection conGmt8, ResultSet rs, TimeZone tz)
      throws SQLException {

    assertEquals("2010-01-12T01:55:12Z", rs.getObject(1, OffsetDateTime.class).toString());

    conGmt8.createStatement().execute("TRUNCATE TABLE DateTimeCodec3");
    try (PreparedStatement prep =
        conGmt8.prepareStatement("INSERT INTO DateTimeCodec3 values (?,?)")) {
      prep.setInt(1, -2);
      prep.setString(2, "2010-01-12 01:55:12");
      prep.execute();

      prep.setInt(1, 1);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T01:55:12Z"));
      prep.execute();

      prep.setInt(1, 2);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T01:55:12-01:00"));
      prep.execute();

      prep.setInt(1, 3);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T01:55:12Z"));
      prep.execute();

      prep.setInt(1, 4);
      prep.setObject(2, OffsetDateTime.parse("2010-01-12T17:55:12+04:00"));
      prep.execute();

      prep.setInt(1, 5);
      prep.setObject(2, Instant.parse("2010-01-12T17:55:13.152Z"));
      prep.execute();
    }
    conGmt8.commit();

    java.sql.Statement stmt = conGmt8.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prepStmt =
        conGmt8.prepareStatement(
            "select * from DateTimeCodec3",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)) {
      rs = prepStmt.executeQuery();
      rs.next();
      assertEquals("2010-01-12T01:55:12Z", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 01:55:12.000000", rs.getString(2));

      rs.next();
      assertEquals("2010-01-12T01:55:12Z", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 01:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263261312000L, rs.getTimestamp(2).getTime());
      assertEquals(
          "2010-01-12 02:55:12.0",
          rs.getTimestamp(2, Calendar.getInstance(TimeZone.getTimeZone("GMT-1:00"))).toString());
      assertEquals("2010-01-12 01:55:12.000000", rs.getString(2));
      assertEquals("2010-01-12", rs.getDate(2).toString());
      assertEquals(
          "2010-01-12",
          rs.getDate(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).toString());
      assertEquals("2010-01-12T01:55:12", rs.getObject(2, LocalDateTime.class).toString());

      rs.next();
      assertEquals("2010-01-12T02:55:12Z", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 02:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263264912000L, rs.getTimestamp(2).getTime());
      assertEquals("2010-01-12 02:55:12.000000", rs.getString(2));
      assertEquals("2010-01-12", rs.getDate(2).toString());

      rs.next();
      assertEquals("2010-01-12T01:55:12Z", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 01:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263261312000L, rs.getTimestamp(2).getTime());
      assertEquals("2010-01-12 01:55:12.000000", rs.getString(2));
      assertEquals("2010-01-12", rs.getDate(2).toString());

      rs.next();
      assertEquals("2010-01-12T13:55:12Z", rs.getObject(2, OffsetDateTime.class).toString());
      assertEquals("2010-01-12 13:55:12.0", rs.getTimestamp(2).toString());
      assertEquals(1263304512000L, rs.getTimestamp(2).getTime());
      assertEquals("2010-01-12 13:55:12.000000", rs.getString(2));
      assertEquals("2010-01-12", rs.getDate(2).toString());

      rs.next();
      assertEquals("2010-01-12T17:55:13.152Z", rs.getObject(2, Instant.class).toString());
    }
    conGmt8.rollback();
  }

  public void getDateTimezoneTestNormal(Connection conAuto, ResultSet rs) throws SQLException {

    assertEquals("2010-01-12 01:55:12.0", rs.getObject(1, Timestamp.class).toString());

    conAuto.createStatement().execute("TRUNCATE TABLE DateTimeCodec3");
    try (PreparedStatement prep =
        conAuto.prepareStatement("INSERT INTO DateTimeCodec3 values (?,?)")) {
      prep.setInt(1, 5);
      prep.setString(2, "2010-01-12 01:55:12");
      prep.execute();

      prep.setInt(1, 6);
      prep.setObject(2, "2010-01-12 11:55:12");
      prep.execute();
    }
    conAuto.commit();

    java.sql.Statement stmt = conAuto.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prepStmt =
        conAuto.prepareStatement(
            "select * from DateTimeCodec3 order by id",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)) {
      rs = prepStmt.executeQuery();
      rs.next();
      assertEquals(5, rs.getInt(1));
      assertEquals("2010-01-12T01:55:12", rs.getObject(2, LocalDateTime.class).toString());
      assertEquals("2010-01-12 01:55:12.000000", rs.getString(2));

      rs.next();

      Timestamp tt = Timestamp.valueOf("2010-01-12 01:55:12");
      int offset = TimeZone.getDefault().getOffset(tt.getTime());
      int offsetHour = offset / (3_600_000);
      if (offsetHour < 0) offsetHour = offsetHour * -1;

      // test might fail if run in timezone with offset not rounded to hours
      if (offsetHour == 0) {
        assertEquals("2010-01-12T11:55:12Z", rs.getObject(2, OffsetDateTime.class).toString());
      } else {
        assertEquals(
            "2010-01-12T11:55:12"
                + ((offset < 0) ? "-" : "+")
                + ((offsetHour < 10) ? "0" : offsetHour / 10)
                + (offsetHour % 10)
                + ":00",
            rs.getObject(2, OffsetDateTime.class).toString());
      }
      assertEquals("2010-01-12 11:55:12.0", rs.getTimestamp(2).toString());
    }
    conAuto.rollback();
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
    int hourOffset =
        TimeZone.getDefault()
                .getOffset(
                    LocalDateTime.parse("2010-01-12T01:55:12")
                            .atZone(TimeZone.getDefault().toZoneId())
                            .toEpochSecond()
                        * 1000)
            / 3600000;
    int hourPlusOffset = (25 + hourOffset) % 24;
    assertEquals(
        Time.valueOf((hourPlusOffset < 10 ? "0" + hourPlusOffset : hourPlusOffset) + ":55:12")
            .toString(),
        rs.getTime(1, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).toString());
    assertFalse(rs.wasNull());

    assertEquals(Time.valueOf("01:55:12").toString(), rs.getTime(1).toString());
    assertFalse(rs.wasNull());

    assertEquals(
        6913212, rs.getTime(2, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Time.valueOf("01:55:13").getTime() + 212, rs.getTime(2).getTime());
    assertFalse(rs.wasNull());
    assertEquals(Time.valueOf("18:30:12").getTime() + 550, rs.getTime(3).getTime());
    assertFalse(rs.wasNull());
    assertNull(rs.getTime(4));
    assertTrue(rs.wasNull());
    if (isMariaDBServer()) {
      rs.next();
      assertNull(rs.getTime(1));
      assertTrue(rs.wasNull());
    }
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
    assertEquals(Duration.parse("PT265H55M12S"), rs.getObject(1, Duration.class));
    assertEquals(Duration.parse("PT1H55M13.212345S"), rs.getObject(2, Duration.class));
    assertNull(rs.getObject(4, Duration.class));
    if (isMariaDBServer()) {
      rs.next();
      assertNull(rs.getObject(1, Duration.class));
      assertTrue(rs.wasNull());
    }
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

  public void getLocalTime(ResultSet rs) throws SQLException {
    assertEquals(LocalTime.parse("01:55:12"), rs.getObject(1, LocalTime.class));
    assertFalse(rs.wasNull());
    assertEquals(LocalTime.parse("01:55:13.212345"), rs.getObject(2, LocalTime.class));
    assertFalse(rs.wasNull());
    assertEquals(LocalTime.parse("18:30:12.55"), rs.getObject(3, LocalTime.class));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4, LocalTime.class));
    assertTrue(rs.wasNull());
    if (isMariaDBServer()) {
      rs.next();
      assertNull(rs.getTime(1));
      assertTrue(rs.wasNull());
      assertNull(rs.getObject(1, LocalTime.class));
      assertTrue(rs.wasNull());
    }
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
    assertFalse(rs.wasNull());
    assertEquals(LocalDate.parse("9999-12-31"), rs.getObject(3, LocalDate.class));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4, LocalTime.class));
    assertTrue(rs.wasNull());
    if (isMariaDBServer()) {
      rs.next();
      assertNull(rs.getObject(1, LocalDate.class));
      assertTrue(rs.wasNull());
      assertNull(rs.getDate(1));
      assertTrue(rs.wasNull());
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
    assertFalse(rs.wasNull());
    assertEquals(Timestamp.valueOf("2010-01-12 01:55:12").getTime(), rs.getTimestamp(1).getTime());
    assertFalse(rs.wasNull());

    Timestamp t1 = Timestamp.valueOf("1000-01-01 01:55:13.212345");
    Timestamp t2 = rs.getTimestamp(2);
    assertEquals(
        Timestamp.valueOf("1000-01-01 01:55:13.212345").getTime(), rs.getTimestamp(2).getTime());
    assertFalse(rs.wasNull());
    assertEquals(
        Timestamp.valueOf("9999-12-31 18:30:12.55").getTime(), rs.getTimestamp(3).getTime());
    assertFalse(rs.wasNull());
    assertNull(rs.getTimestamp(4));
    assertTrue(rs.wasNull());
    if (isMariaDBServer()) {
      rs.next();
      assertNull(rs.getTimestamp(1));
      assertTrue(rs.wasNull());
      assertNull(rs.getTimestamp(2));
      assertTrue(rs.wasNull());
      assertNull(rs.getObject(2, Timestamp.class));
      assertTrue(rs.wasNull());
      assertEquals(
          Timestamp.valueOf("9999-12-31 00:00:00.00").getTime(), rs.getTimestamp(3).getTime());
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
    assertFalse(rs.wasNull());
    assertEquals(LocalDateTime.parse("2010-01-12T01:55:12"), rs.getObject(1, LocalDateTime.class));
    assertFalse(rs.wasNull());
    assertEquals(
        LocalDateTime.parse("1000-01-01T01:55:13.212345"), rs.getObject(2, LocalDateTime.class));
    assertFalse(rs.wasNull());
    assertEquals(
        LocalDateTime.parse("9999-12-31T18:30:12.55"), rs.getObject(3, LocalDateTime.class));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4, LocalDateTime.class));
    assertTrue(rs.wasNull());
    if (isMariaDBServer()) {
      rs.next();
      assertNull(rs.getTimestamp(1));
      assertTrue(rs.wasNull());
      assertNull(rs.getTimestamp(2));
      assertTrue(rs.wasNull());
      assertNull(rs.getObject(1, LocalDateTime.class));
      assertTrue(rs.wasNull());
      assertNull(rs.getObject(2, LocalDateTime.class));
      assertTrue(rs.wasNull());
      assertEquals(
          LocalDateTime.parse("9999-12-31T00:00:00.00"), rs.getObject(3, LocalDateTime.class));
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
        ZonedDateTime.of(LocalDateTime.parse("2010-01-12T01:55:12"), ZoneId.systemDefault())
            .toInstant(),
        rs.getObject(1, Instant.class));
    assertFalse(rs.wasNull());
    assertEquals(
        ZonedDateTime.of(LocalDateTime.parse("1000-01-01T01:55:13.212345"), ZoneId.systemDefault())
            .toInstant(),
        rs.getObject(2, Instant.class));
    assertFalse(rs.wasNull());
    assertEquals(
        ZonedDateTime.of(LocalDateTime.parse("9999-12-31T18:30:12.55"), ZoneId.systemDefault())
            .toInstant(),
        rs.getObject(3, Instant.class));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4, Instant.class));
    assertTrue(rs.wasNull());
    if (isMariaDBServer()) {
      rs.next();
      assertNull(rs.getTimestamp(1));
      assertTrue(rs.wasNull());
      assertNull(rs.getTimestamp(2));
      assertTrue(rs.wasNull());
      assertEquals(
          ZonedDateTime.of(LocalDateTime.parse("9999-12-31T00:00:00.00"), ZoneId.systemDefault())
              .toInstant(),
          rs.getObject(3, Instant.class));
    }
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
    assertFalse(rs.wasNull());
    assertEquals(
        OffsetDateTime.ofInstant(
            Timestamp.valueOf("2010-01-12 01:55:12").toInstant(), ZoneId.systemDefault()),
        rs.getObject(1, OffsetDateTime.class));
    assertFalse(rs.wasNull());

    LocalDateTime l = LocalDateTime.parse("1000-01-01T01:55:13.212345");
    assertEquals(
        OffsetDateTime.of(l, ZoneId.systemDefault().getRules().getOffset(l)),
        rs.getObject(2, OffsetDateTime.class));
    assertFalse(rs.wasNull());

    assertEquals(
        OffsetDateTime.ofInstant(
            Timestamp.valueOf("9999-12-31 18:30:12.55").toInstant(), ZoneId.systemDefault()),
        rs.getObject(3, OffsetDateTime.class));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4, OffsetDateTime.class));
    assertTrue(rs.wasNull());
    if (isMariaDBServer()) {
      rs.next();
      assertNull(rs.getObject(1, OffsetDateTime.class));
      assertTrue(rs.wasNull());
      assertNull(rs.getObject(2, OffsetDateTime.class));
      assertTrue(rs.wasNull());
      assertEquals(
          OffsetDateTime.ofInstant(
              Timestamp.valueOf("9999-12-31 00:00:00.00").toInstant(), ZoneId.systemDefault()),
          rs.getObject(3, OffsetDateTime.class));
    }
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
        "Data type DATETIME cannot be decoded as Stream");
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
        "Data type DATETIME cannot be decoded as Stream");
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
        "Data type DATETIME cannot be decoded as Stream");
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
        SQLException.class, () -> rs.getBytes(1), "Data type DATETIME cannot be decoded as byte[]");
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
        "Data type DATETIME cannot be decoded as Reader");
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
        "Data type DATETIME cannot be decoded as Reader");
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
        SQLException.class, () -> rs.getBlob(1), "Data type DATETIME cannot be decoded as Blob");
  }

  @Test
  public void getBigInteger() throws SQLException {
    getBigInteger(get());
  }

  @Test
  public void getBigIntegerPrepared() throws SQLException {
    getBigInteger(getPrepare(sharedConn));
    getBigInteger(getPrepare(sharedConnBinary));
  }

  private void getBigInteger(ResultSet res) {
    CompleteResult rs = (CompleteResult) res;
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBigInteger(1),
        "Data type DATETIME cannot be decoded as BigInteger");
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
        SQLException.class, () -> rs.getClob(1), "Data type DATETIME cannot be decoded as Clob");
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
        SQLException.class, () -> rs.getNClob(1), "Data type DATETIME cannot be decoded as Clob");
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = get();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("DATETIME", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.sql.Timestamp", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.TIMESTAMP, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    // https://jira.mariadb.org/browse/XPT-273
    if (!isXpand()) {
      assertEquals(19, meta.getPrecision(1));
      assertEquals(19, meta.getColumnDisplaySize(1));
    }
  }

  @Test
  public void sendParam() throws SQLException {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws SQLException {
    java.sql.Statement stmt =
        con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    stmt.execute("TRUNCATE TABLE DateTimeCodec2");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    LocalDateTime ldtNow = LocalDateTime.parse("2022-04-15T19:49:41.398057");
    OffsetDateTime offsetDtUtc =
        OffsetDateTime.of(ldtNow, ZoneId.of("UTC").getRules().getOffset(ldtNow));
    OffsetDateTime offsetDtCurrent =
        OffsetDateTime.of(ldtNow, ZoneId.systemDefault().getRules().getOffset(ldtNow));

    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO DateTimeCodec2(t1) VALUES (?)")) {
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
      prep.setObject(1, LocalDateTime.parse("2010-01-12T01:55:12.987765"), Types.TIMESTAMP);
      prep.execute();
      prep.setObject(1, "2010-01-12 01:55:12.987765", Types.TIMESTAMP);
      prep.execute();
      prep.setObject(1, "0000-00-00 00:00:00", Types.TIMESTAMP);
      prep.execute();
      prep.setObject(1, LocalDateTime.parse("2010-01-12T01:56:12.456"), Types.TIMESTAMP);
      prep.execute();
      prep.setObject(
          1,
          LocalDateTime.parse("2011-01-12T01:55:12").atZone(ZoneId.systemDefault()),
          Types.TIMESTAMP);
      prep.execute();
      prep.setObject(
          1,
          LocalDateTime.parse("2011-01-12T01:55:12.456").atZone(ZoneId.systemDefault()),
          Types.TIMESTAMP);
      prep.execute();
      prep.setObject(
          1, LocalDateTime.parse("2012-01-12T01:55:12").atZone(ZoneId.of("UTC")), Types.TIMESTAMP);
      prep.execute();
      prep.setObject(
          1,
          LocalDateTime.parse("2012-01-12T01:55:12.456").atZone(ZoneId.of("UTC")),
          Types.TIMESTAMP);
      prep.execute();
      prep.setTimestamp(1, Timestamp.valueOf("2015-12-12 01:55:12"));
      prep.execute();
      prep.setTimestamp(1, Timestamp.valueOf("2015-12-12 01:55:12.654"));
      prep.execute();
      prep.setObject(1, new java.util.Date(Timestamp.valueOf("2016-12-18 01:55:12.2").getTime()));
      prep.execute();
      prep.setObject(1, Timestamp.valueOf("2016-12-12 01:55:12.654"));
      prep.execute();
      prep.setObject(1, Instant.ofEpochSecond(10, 654000));
      prep.execute();
      prep.setObject(1, Instant.ofEpochSecond(12));
      prep.execute();
      prep.setObject(1, offsetDtUtc);
      prep.execute();
      prep.setObject(1, offsetDtCurrent);
      prep.execute();
      assertThrowsContains(
          SQLException.class,
          () -> prep.setObject(1, "2010-aaa", Types.TIMESTAMP),
          "Could not convert [2010-aaa] to java.sql.Type 93");
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM DateTimeCodec2");
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2010-01-12"), rs.getDate(2));
    rs.updateDate(2, null);
    rs.updateRow();
    assertNull(rs.getDate(2));

    assertTrue(rs.next());
    assertNull(rs.getString(2));
    rs.updateDate(2, Date.valueOf("2011-01-12"));
    rs.updateRow();
    assertEquals(Date.valueOf("2011-01-12"), rs.getDate(2));

    assertTrue(rs.next());
    assertEquals(Date.valueOf("2010-01-13"), rs.getDate(2));
    rs.updateObject(2, null);
    rs.updateRow();
    assertNull(rs.getDate(2));

    assertTrue(rs.next());
    assertNull(rs.getString(2));
    rs.updateObject(2, Date.valueOf("2021-01-12"));
    rs.updateRow();
    assertEquals(Date.valueOf("2021-01-12"), rs.getDate(2));

    assertTrue(rs.next());
    assertEquals(Date.valueOf("2010-01-14"), rs.getDate(2));
    rs.updateObject(2, LocalDateTime.parse("2021-01-12T01:55:12.347654"), Types.TIMESTAMP);
    rs.updateRow();
    assertEquals(
        LocalDateTime.parse("2021-01-12T01:55:12.347654"), rs.getObject(2, LocalDateTime.class));
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    rs.updateTimestamp(2, Timestamp.valueOf("2015-12-12 01:55:12.654"));
    rs.updateRow();
    assertEquals(Timestamp.valueOf("2015-12-12 01:55:12.654"), rs.getTimestamp(2));
    assertTrue(rs.next());
    assertEquals(
        LocalDateTime.parse("2010-01-12T01:55:12.987765"), rs.getObject(2, LocalDateTime.class));
    rs.updateTimestamp("t1", Timestamp.valueOf("2015-12-12 01:55:12.654321"));
    rs.updateRow();
    assertEquals(Timestamp.valueOf("2015-12-12 01:55:12.654321"), rs.getTimestamp(2));
    assertTrue(rs.next());
    assertEquals(
        LocalDateTime.parse("2010-01-12T01:55:12.987765"), rs.getObject(2, LocalDateTime.class));
    assertTrue(rs.next());
    assertNull(rs.getObject(2, LocalDateTime.class));

    rs = stmt.executeQuery("SELECT * FROM DateTimeCodec2");
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2011-01-12"), rs.getDate(2));
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    assertTrue(rs.next());
    assertEquals(Date.valueOf("2021-01-12"), rs.getDate(2));
    assertTrue(rs.next());
    assertEquals(
        LocalDateTime.parse("2021-01-12T01:55:12.347654"), rs.getObject(2, LocalDateTime.class));
    assertTrue(rs.next());
    assertEquals(Timestamp.valueOf("2015-12-12 01:55:12.654"), rs.getTimestamp(2));

    assertTrue(rs.next());
    assertEquals(Timestamp.valueOf("2015-12-12 01:55:12.654321"), rs.getTimestamp(2));
    assertTrue(rs.next());
    assertEquals(Timestamp.valueOf("2010-01-12 01:55:12.987765"), rs.getTimestamp(2));
    assertTrue(rs.next());
    assertTrue(rs.next());
    assertEquals(
        LocalDateTime.parse("2010-01-12T01:56:12.456"), rs.getObject(2, LocalDateTime.class));

    assertTrue(rs.next());
    assertEquals(LocalDateTime.parse("2011-01-12T01:55:12"), rs.getObject(2, LocalDateTime.class));
    assertTrue(rs.next());
    assertEquals(
        LocalDateTime.parse("2011-01-12T01:55:12.456"), rs.getObject(2, LocalDateTime.class));
    assertTrue(rs.next());
    assertEquals(
        LocalDateTime.parse("2012-01-12T01:55:12").atZone(ZoneId.of("UTC")),
        rs.getObject(2, ZonedDateTime.class).withZoneSameInstant(ZoneId.of("UTC")));
    assertTrue(rs.next());
    assertEquals(
        LocalDateTime.parse("2012-01-12T01:55:12.456").atZone(ZoneId.of("UTC")),
        rs.getObject(2, ZonedDateTime.class).withZoneSameInstant(ZoneId.of("UTC")));

    assertTrue(rs.next());
    assertEquals(Timestamp.valueOf("2015-12-12 01:55:12"), rs.getTimestamp(2));
    assertTrue(rs.next());
    assertEquals(Timestamp.valueOf("2015-12-12 01:55:12.654"), rs.getTimestamp(2));
    assertTrue(rs.next());
    assertEquals(Timestamp.valueOf("2016-12-18 01:55:12.2"), rs.getTimestamp(2));
    assertTrue(rs.next());
    assertEquals(Timestamp.valueOf("2016-12-12 01:55:12.654"), rs.getTimestamp(2));
    assertEquals(
        new java.util.Date(Timestamp.valueOf("2016-12-12 01:55:12.654").getTime()),
        rs.getObject(2, java.util.Date.class));
    assertTrue(rs.next());
    assertEquals(Timestamp.from(Instant.ofEpochSecond(10, 654000)), rs.getTimestamp(2));
    assertTrue(rs.next());
    assertEquals(Timestamp.from(Instant.ofEpochSecond(12)), rs.getTimestamp(2));
    assertTrue(rs.next());
    assertEquals(
        ldtNow
            .atZone(ZoneId.of("UTC"))
            .withZoneSameInstant(ZoneId.systemDefault())
            .toOffsetDateTime(),
        rs.getObject(2, OffsetDateTime.class));
    assertEquals(
        ldtNow.atZone(ZoneId.of("UTC")).withZoneSameInstant(ZoneId.systemDefault()),
        rs.getObject(2, ZonedDateTime.class));
    assertEquals(
        ldtNow
            .atZone(ZoneId.of("UTC"))
            .withZoneSameInstant(ZoneId.systemDefault())
            .toLocalDateTime(),
        rs.getObject(2, LocalDateTime.class));
    assertTrue(rs.next());
    assertEquals(offsetDtCurrent, rs.getObject(2, OffsetDateTime.class));
    assertEquals(ldtNow.atZone(ZoneId.systemDefault()), rs.getObject(2, ZonedDateTime.class));
    assertEquals(ldtNow, rs.getObject(2, LocalDateTime.class));
    con.commit();
  }
}
