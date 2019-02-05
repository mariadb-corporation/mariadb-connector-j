/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class DateTest extends BaseTest {

  private static final String TIMESTAMP_1 = "2015-05-13 08:15:14";
  private static final String TIMESTAMP_YEAR_ZERO = "0000-11-15 10:15:22";

  /**
   * Initialization.
   *
   * @throws SQLException exception
   */
  @BeforeClass()
  public static void initClass() throws SQLException {
    createTable("dtest", "d date");
    createTable("date_test2",
        "id int not null primary key auto_increment, d_from datetime ,d_to datetime");
    createTable("timetest", "t time");
    createTable("timetest2", "t time");
    createTable("timestampzerotest", "ts timestamp, dt datetime, dd date");
    createTable("dtest", "d datetime");
    createTable("dtest2", "d date");
    createTable("dtest3", "d date");
    createTable("dtest4", "d  time");
    createTable("date_test3", " x date");
    createTable("date_test4", "x date");
    if (doPrecisionTest) {
      createTable("timestampAsDate", "ts timestamp(6), dt datetime(6), dd date");
    }
  }

  @Test
  public void dateTestLegacy() throws SQLException {
    dateTest(true);
  }

  @Test
  public void dateTestWithoutLegacy() throws SQLException {
    dateTest(false);
  }

  /**
   * Date testing.
   *
   * @param useLegacy use legacy client side timezone or server side timezone.
   * @throws SQLException exception
   */
  public void dateTest(boolean useLegacy) throws SQLException {
    Assume.assumeFalse(sharedIsRewrite());
    try (Connection connection = setConnection("&useLegacyDatetimeCode=" + useLegacy
        + "&serverTimezone=+5:00&maximizeMysqlCompatibility=false&useServerPrepStmts=true")) {

      setSessionTimeZone(connection, "+5:00");
      createTable("date_test",
          "id int not null primary key auto_increment, d_test date,dt_test datetime, "
              + "t_test time");
      Statement stmt = connection.createStatement();
      Date date = Date.valueOf("2009-01-17");
      Timestamp timestamp = Timestamp.valueOf("2009-01-17 15:41:01");
      Time time = Time.valueOf("23:59:59");
      PreparedStatement ps = connection
          .prepareStatement("insert into date_test (d_test, dt_test, t_test) "
              + "values (?,?,?)");
      ps.setDate(1, date);
      ps.setTimestamp(2, timestamp);
      ps.setTime(3, time);
      ps.executeUpdate();
      ResultSet rs = stmt.executeQuery("select d_test, dt_test, t_test from date_test");
      assertEquals(true, rs.next());
      Date date2 = rs.getDate(1);
      Date date3 = rs.getDate("d_test");
      Time time2 = rs.getTime(3);
      assertEquals(date.toString(), date2.toString());
      assertEquals(date.toString(), date3.toString());
      assertEquals(time.toString(), time2.toString());
      Time time3 = rs.getTime("t_test");
      assertEquals(time.toString(), time3.toString());
      Timestamp timestamp2 = rs.getTimestamp(2);
      assertEquals(timestamp.toString(), timestamp2.toString());
      Timestamp timestamp3 = rs.getTimestamp("dt_test");
      assertEquals(timestamp.toString(), timestamp3.toString());
    }

  }

  @Test
  public void dateRangeTest() throws SQLException {
    PreparedStatement ps = sharedConnection
        .prepareStatement("insert into date_test2 (id, d_from, d_to) values "
            + "(1, ?,?)");
    Timestamp timestamp1 = Timestamp.valueOf("2009-01-17 15:41:01");
    Timestamp timestamp2 = Timestamp.valueOf("2015-01-17 15:41:01");
    ps.setTimestamp(1, timestamp1);
    ps.setTimestamp(2, timestamp2);
    ps.executeUpdate();
    PreparedStatement ps1 = sharedConnection.prepareStatement("select d_from, d_to from date_test2 "
        + "where d_from <= ? and d_to >= ?");
    Timestamp timestamp3 = Timestamp.valueOf("2014-01-17 15:41:01");
    ps1.setTimestamp(1, timestamp3);
    ps1.setTimestamp(2, timestamp3);
    ResultSet rs = ps1.executeQuery();
    assertEquals(true, rs.next());
    Timestamp ts1 = rs.getTimestamp(1);
    Timestamp ts2 = rs.getTimestamp(2);
    assertEquals(ts1.toString(), timestamp1.toString());
    assertEquals(ts2.toString(), timestamp2.toString());

  }

  @Test(expected = SQLException.class)
  public void dateTest2() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    ResultSet rs = stmt.executeQuery("select 1");
    assertTrue(rs.next());
    rs.getDate(1);
  }

  @Test(expected = SQLException.class)
  public void dateTest3() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    ResultSet rs = stmt.executeQuery("select 1 as a");
    assertTrue(rs.next());
    rs.getDate("a");
  }

  @Test(expected = SQLException.class)
  public void timeTest3() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    ResultSet rs = stmt.executeQuery("select 'aaa' as a");
    assertTrue(rs.next());
    rs.getTimestamp("a");
  }

  @Test
  public void yearTest() throws SQLException {
    Assume.assumeTrue(isMariadbServer());
    createTable("yeartest", "y1 year, y2 year(2)");
    sharedConnection.createStatement()
        .execute("insert into yeartest values (null, null), (1901, 70), (0, 0), "
            + "(2155, 69)");
    Statement stmt = sharedConnection.createStatement();
    ResultSet rs = stmt.executeQuery("select * from yeartest");

    Date[] data1 = new Date[]{null, Date.valueOf("1901-01-01"), Date.valueOf("0000-01-01"),
        Date.valueOf("2155-01-01")};
    Date[] data2 = new Date[]{null, Date.valueOf("1970-01-01"), Date.valueOf("2000-01-01"),
        Date.valueOf("2069-01-01")};
    checkDateResult(data1, data2, rs);

    //CONJ-282
    PreparedStatement preparedStatement = sharedConnection
        .prepareStatement("SELECT * FROM yeartest");
    rs = preparedStatement.executeQuery();
    checkDateResult(data1, data2, rs);
  }

  private void checkDateResult(Date[] data1, Date[] data2, ResultSet rs) throws SQLException {
    int count = 0;
    while (rs.next()) {
      assertEquals(data1[count], rs.getObject(1));
      assertEquals(data2[count], rs.getObject(2));
      assertEquals(data1[count], rs.getDate(1));
      assertEquals(data2[count], rs.getDate(2));
      count++;
    }
  }

  @Test
  public void timeTestLegacy() {
    try (Connection connection = setConnection(
        "&useLegacyDatetimeCode=true&serverTimezone=+05:00")) {

      setSessionTimeZone(connection, "+05:00");
      connection.createStatement()
          .execute("insert into timetest values (null), ('-838:59:59'), ('00:00:00'), "
              + "('838:59:59')");
      Time[] data = new Time[]{null, Time.valueOf("-838:59:59"), Time.valueOf("00:00:00"),
          Time.valueOf("838:59:59")};
      Statement stmt = connection.createStatement();
      try (ResultSet rs = stmt.executeQuery("select * from timetest")) {
        testTime(rs, data);
      }

      PreparedStatement pstmt = connection.prepareStatement("select * from timetest");
      try (ResultSet rs = pstmt.executeQuery()) {
        testTime(rs, data);
      }

      try (ResultSet rs = stmt.executeQuery("select '11:11:11'")) {
        testTime11(rs);
      }

      PreparedStatement pstmt2 = connection.prepareStatement("select TIME('11:11:11') ");
      try (ResultSet rs = pstmt2.executeQuery()) {
        testTime11(rs);
      }
    } catch (SQLException sqle) {
      sqle.printStackTrace();
      fail();
    }
  }

  @Test
  public void timeTest() {
    try (Connection connection = setConnection(
        "&useLegacyDatetimeCode=false&serverTimezone=+5:00")) {
      setSessionTimeZone(connection, "+5:00");
      connection.createStatement()
          .execute("insert into timetest2 values (null), ('00:00:00'), ('23:59:59')");
      Time[] data = new Time[]{null, Time.valueOf("00:00:00"), Time.valueOf("23:59:59")};

      Statement stmt = connection.createStatement();
      try (ResultSet rs = stmt.executeQuery("select * from timetest2")) {
        testTime(rs, data);
      }

      PreparedStatement pstmt = connection.prepareStatement("select * from timetest2");
      try (ResultSet rs = pstmt.executeQuery()) {
        testTime(rs, data);
      }

      try (ResultSet rs = stmt.executeQuery("select '11:11:11'")) {
        testTime11(rs);
      }

      PreparedStatement pstmt2 = connection.prepareStatement("select TIME('11:11:11') ");
      try (ResultSet rs = pstmt2.executeQuery()) {
        testTime11(rs);
      }
    } catch (SQLException sqle) {
      sqle.printStackTrace();
      fail();
    }
  }

  private void testTime(ResultSet rs, Time[] data) throws SQLException {
    int count = 0;
    while (rs.next()) {
      Time t1 = data[count];
      Time t2 = (Time) rs.getObject(1);
      assertEquals(t1, t2);
      count++;
    }
  }

  private void testTime11(ResultSet rs) throws SQLException {
    assertTrue(rs.next());
    Calendar cal = Calendar.getInstance();
    assertEquals("11:11:11", rs.getTime(1, cal).toString());
  }

  @Test
  public void timestampZeroTest() throws SQLException {
    Assume.assumeTrue(isMariadbServer());
    String timestampZero = "0000-00-00 00:00:00";
    String dateZero = "0000-00-00";
    sharedConnection.createStatement().execute("insert into timestampzerotest values ('"
        + timestampZero + "', '" + timestampZero + "', '" + dateZero + "')");
    Statement stmt = sharedConnection.createStatement();
    try (ResultSet rs = stmt.executeQuery("select * from timestampzerotest")) {
      Timestamp ts = null;
      Timestamp datetime = null;
      Date date = null;
      while (rs.next()) {
        assertEquals(null, rs.getObject(1));
        ts = rs.getTimestamp(1);
        assertEquals(rs.wasNull(), true);
        datetime = rs.getTimestamp(2);
        assertEquals(rs.wasNull(), true);
        date = rs.getDate(3);
        assertEquals(rs.wasNull(), true);
      }
      assertEquals(ts, null);
      assertEquals(datetime, null);
      assertEquals(date, null);
    }
  }

  @Test
  public void timestampAsDate() throws SQLException {
    Assume.assumeTrue(doPrecisionTest);

    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);

    Calendar cal2 = Calendar.getInstance();
    cal2.set(Calendar.YEAR, 1970);
    cal2.set(Calendar.MONTH, 0);
    cal2.set(Calendar.DAY_OF_YEAR, 1);

    Calendar cal3 = Calendar.getInstance();
    cal3.set(Calendar.HOUR_OF_DAY, 0);
    cal3.set(Calendar.MINUTE, 0);
    cal3.set(Calendar.SECOND, 0);
    cal3.set(Calendar.MILLISECOND, 0);
    cal3.set(Calendar.YEAR, 1970);
    cal3.set(Calendar.MONTH, 0);
    cal3.set(Calendar.DAY_OF_YEAR, 1);

    Timestamp currentTimeStamp = new Timestamp(System.currentTimeMillis());
    PreparedStatement preparedStatement1 = sharedConnection
        .prepareStatement("/*CLIENT*/ insert into timestampAsDate values (?, ?, ?)");
    preparedStatement1.setTimestamp(1, currentTimeStamp);
    preparedStatement1.setTimestamp(2, currentTimeStamp);
    preparedStatement1.setDate(3, new Date(currentTimeStamp.getTime()));
    preparedStatement1.addBatch();
    preparedStatement1.execute();

    Date dateWithoutTime = new Date(cal.getTimeInMillis());

    ResultSet rs = sharedConnection.createStatement().executeQuery("select * from timestampAsDate");
    checkResult(rs, currentTimeStamp, cal, dateWithoutTime);

    PreparedStatement pstmt = sharedConnection
        .prepareStatement("select * from timestampAsDate where 1 = ?");
    pstmt.setInt(1, 1);
    pstmt.addBatch();
    rs = pstmt.executeQuery();
    checkResult(rs, currentTimeStamp, cal, dateWithoutTime);
  }

  private void checkResult(ResultSet rs, Timestamp currentTimeStamp, Calendar cal,
                           Date dateWithoutTime) throws SQLException {
    if (rs.next()) {
      assertEquals(rs.getTimestamp(1), currentTimeStamp);
      assertEquals(rs.getTimestamp(2), currentTimeStamp);
      assertEquals(rs.getTimestamp(3), new Timestamp(cal.getTimeInMillis()));

      assertEquals(rs.getDate(1), new Date(currentTimeStamp.getTime()));
      assertEquals(rs.getDate(2), new Date(currentTimeStamp.getTime()));
      assertEquals(rs.getDate(3), dateWithoutTime);
      assertEquals(rs.getTime(1), new Time(currentTimeStamp.getTime()));
      assertEquals(rs.getTime(2), new Time(currentTimeStamp.getTime()));
      try {
        rs.getTime(3);
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Cannot read Time using a Types.DATE field"));
      }
    } else {
      fail("Must have a result");
    }
    rs.close();

  }

  @Test
  public void javaUtilDateInPreparedStatementAsTimeStamp() throws Exception {
    java.util.Date currentDate = Calendar.getInstance(TimeZone.getDefault()).getTime();
    PreparedStatement ps = sharedConnection.prepareStatement("insert into dtest values(?)");
    ps.setObject(1, currentDate, Types.TIMESTAMP);
    ps.executeUpdate();
    ResultSet rs = sharedConnection.createStatement().executeQuery("select * from dtest");
    assertTrue(rs.next());
    /* Check that time is correct, up to seconds precision */
    assertTrue(Math.abs((currentDate.getTime() - rs.getTimestamp(1).getTime())) <= 1000);
  }

  @Test
  public void nullTimestampTest() throws SQLException {
    PreparedStatement ps = sharedConnection.prepareStatement("insert into dtest2 values(null)");
    ps.executeUpdate();
    ResultSet rs = sharedConnection.createStatement()
        .executeQuery("select * from dtest2 where d is null");
    assertTrue(rs.next());
    Calendar cal = new GregorianCalendar();
    assertEquals(null, rs.getTimestamp(1, cal));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void javaUtilDateInPreparedStatementAsDate() throws Exception {
    java.util.Date currentDate = Calendar.getInstance(TimeZone.getDefault()).getTime();
    PreparedStatement ps = sharedConnection.prepareStatement("insert into dtest3 values(?)");
    ps.setObject(1, currentDate, Types.DATE);
    ps.executeUpdate();
    ResultSet rs = sharedConnection.createStatement().executeQuery("select * from dtest3");
    assertTrue(rs.next());
    /* Check that time is correct, up to seconds precision */
    assertEquals(currentDate.getYear(), rs.getDate(1).getYear());
    assertEquals(currentDate.getMonth(), rs.getDate(1).getMonth());
    assertEquals(currentDate.getDay(), rs.getDate(1).getDay());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void javaUtilDateInPreparedStatementAsTime() throws Exception {
    java.util.Date currentDate = Calendar.getInstance(TimeZone.getDefault()).getTime();
    PreparedStatement ps = sharedConnection.prepareStatement("insert into dtest4 values(?)");
    ps.setObject(1, currentDate, Types.TIME);
    ps.executeUpdate();
    ResultSet rs = sharedConnection.createStatement().executeQuery("select * from dtest4");
    assertTrue(rs.next());

    Calendar calendar = Calendar.getInstance();
    calendar.setTime(currentDate);
    calendar.set(Calendar.YEAR, 1970);
    calendar.set(Calendar.MONTH, 0);
    calendar.set(Calendar.DAY_OF_MONTH, 1);

    /* Check that time is correct, up to seconds precision */
    assertTrue(Math.abs(calendar.getTimeInMillis() - rs.getTime(1).getTime()) <= 1000);
  }

  @Test
  public void serverTimezone() throws Exception {
    TimeZone tz = TimeZone.getDefault();

    try (Connection connection = setConnection("&serverTimezone=+5:00")) {
      setSessionTimeZone(connection, "+5:00");

      java.util.Date now = new java.util.Date();
      TimeZone canadaTimeZone = TimeZone.getTimeZone("GMT+5:00");

      long clientOffset = tz.getOffset(now.getTime());
      long serverOffset = canadaTimeZone.getOffset(System.currentTimeMillis());
      long totalOffset = serverOffset - clientOffset;
      PreparedStatement ps = connection.prepareStatement("select now()");
      ResultSet rs = ps.executeQuery();
      assertTrue(rs.next());
      Timestamp ts = rs.getTimestamp(1);
      long differenceToServer = ts.getTime() - now.getTime();
      long diff = Math.abs(differenceToServer - totalOffset);
      /* query take less than a second but taking in account server and client time second diff ... */
      assertTrue(diff < 5000);

      ps = connection.prepareStatement("select utc_timestamp(), ?");
      ps.setObject(1, now);
      rs = ps.executeQuery();
      assertTrue(rs.next());
      ts = rs.getTimestamp(1);
      Timestamp ts2 = rs.getTimestamp(2);
      long diff2 = Math.abs(ts.getTime() - ts2.getTime() + clientOffset);
      assertTrue(diff2 < 5000); /* query take less than a second */
    }
  }

  /**
   * Conj-107.
   *
   * @throws SQLException exception
   */
  @Test
  public void timestampMillisecondsTest() throws SQLException {
    Statement statement = sharedConnection.createStatement();

    boolean isMariadbServer = isMariadbServer();
    if (isMariadbServer) {
      createTable("tt", "id decimal(10), create_time datetime(6)");
      statement.execute("INSERT INTO tt (id, create_time) VALUES (1,'2013-07-18 13:44:22.123456')");
    } else {
      createTable("tt", "id decimal(10), create_time datetime");
      statement.execute("INSERT INTO tt (id, create_time) VALUES (1,'2013-07-18 13:44:22')");
    }
    PreparedStatement ps = sharedConnection
        .prepareStatement("insert into tt (id, create_time) values (?,?)");
    ps.setInt(1, 2);
    Timestamp writeTs = new Timestamp(1273017612999L);
    Timestamp writeTsWithoutMilliSec = new Timestamp(1273017612999L);
    ps.setTimestamp(2, writeTs);
    ps.execute();
    ResultSet rs = statement.executeQuery("SELECT * FROM tt");
    assertTrue(rs.next());
    if (isMariadbServer) {
      assertTrue("2013-07-18 13:44:22.123456".equals(rs.getString(2)));
    } else {
      assertTrue("2013-07-18 13:44:22.0".equals(rs.getString(2)));
    }
    assertTrue(rs.next());
    Timestamp readTs = rs.getTimestamp(2);
    if (isMariadbServer) {
      assertEquals(writeTs, readTs);
    } else {
      assertEquals(writeTs, writeTsWithoutMilliSec);
    }
  }

  @Test
  public void dateTestWhenServerDifference() throws Throwable {
    try (Connection connection = setConnection("&serverTimezone=UTC")) {
      try (PreparedStatement pst = connection
          .prepareStatement("insert into date_test3 values (?)")) {
        Date date = Date.valueOf("2013-02-01");
        pst.setDate(1, date);
        pst.execute();

        try (PreparedStatement pst2 = connection
            .prepareStatement("select x from date_test3 WHERE x = ?")) {
          pst2.setDate(1, date);
          try (ResultSet rs = pst2.executeQuery()) {
            assertTrue(rs.next());
            Date dd = rs.getDate(1);
            assertEquals(dd, date);
          }
        }
      }
    }
  }

  @Test
  public void dateTestWhenServerDifferenceClient() throws Throwable {
    try (Connection connection = setConnection("&serverTimezone=UTC")) {
      try (PreparedStatement pst = connection
          .prepareStatement("/*CLIENT*/insert into date_test4 values (?)")) {
        Date date = Date.valueOf("2013-02-01");
        pst.setDate(1, date);
        pst.execute();

        try (PreparedStatement pst2 = connection
            .prepareStatement("/*CLIENT*/ select x from date_test4 WHERE x = ?")) {
          pst2.setDate(1, date);
          try (ResultSet rs = pst2.executeQuery()) {
            assertTrue(rs.next());
            Date dd = rs.getDate(1);
            assertEquals(dd, date);
          }
        }
      }
    }
  }

  /**
   * Conj-267 : null pointer exception getting zero date.
   */
  @Test
  public void nullDateString() throws Throwable {

    createTable("date_test5", "x date");
    Statement stmt = sharedConnection.createStatement();
    try {
      stmt.execute("INSERT INTO date_test5 (x) VALUES ('0000-00-00')");
      PreparedStatement pst = sharedConnection
          .prepareStatement("SELECT * FROM date_test5 WHERE 1 = ?");
      pst.setInt(1, 1);
      ResultSet rs = pst.executeQuery();
      assertTrue(rs.next());
      if (sharedUsePrepare()) {
        assertNull(rs.getString(1));
        assertTrue(rs.wasNull());
        assertNull(rs.getDate(1));
        assertTrue(rs.wasNull());
      } else {
        assertEquals("0000-00-00", rs.getString(1));
        assertFalse(rs.wasNull());
        assertNull(rs.getDate(1));
        assertTrue(rs.wasNull());
      }
    } catch (SQLDataException sqldataException) {
      //'0000-00-00' doesn't work anymore on mysql 5.7.
    }
  }

  /**
   * Conj-317 : null pointer exception on getDate on null timestamp.
   */
  @Test
  public void nullDateFromTimestamp() throws Throwable {
    Assume.assumeTrue(isMariadbServer());

    createTable("nulltimestamp", "ts timestamp(6) NULL ");
    Statement stmt = sharedConnection.createStatement();
    try {
      stmt.execute("INSERT INTO nulltimestamp (ts) VALUES ('0000-00-00'), (null)");

      PreparedStatement pst = sharedConnection
          .prepareStatement("SELECT * FROM nulltimestamp WHERE 1 = ?");
      pst.setInt(1, 1);
      ResultSet rs = pst.executeQuery();
      assertTrue(rs.next());
      if (sharedUsePrepare()) {
        assertEquals(null, rs.getString(1));
      } else {
        assertTrue(rs.getString(1).contains("0000-00-00 00:00:00"));
      }
      assertNull(rs.getDate(1));
      assertNull(rs.getTimestamp(1));
      assertNull(rs.getTime(1));

      assertTrue(rs.next());
      assertNull(rs.getString(1));
      assertNull(rs.getDate(1));
      assertNull(rs.getTimestamp(1));
      assertNull(rs.getTime(1));

    } catch (SQLDataException sqldataException) {
      //'0000-00-00' doesn't work anymore on mysql 5.7.
    }
  }

  /**
   * CONJ-388 : getString on a '0000-00-00 00:00:00' must not return null.
   *
   * @throws SQLException if exception occur
   */
  @Test
  public void getZeroDateString() throws SQLException {
    Assume.assumeTrue(isMariadbServer());
    createTable("zeroTimestamp", "ts timestamp NULL ");
    try (Statement statement = sharedConnection.createStatement()) {
      statement.execute("INSERT INTO zeroTimestamp values ('0000-00-00 00:00:00')");
      try (PreparedStatement preparedStatement = sharedConnection
          .prepareStatement("SELECT * from zeroTimestamp")) {
        ResultSet resultSet = preparedStatement.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(null, resultSet.getDate(1));
        if (sharedUsePrepare()) {
          assertEquals(null, resultSet.getString(1));
          assertTrue(resultSet.wasNull());
        } else {
          assertTrue(resultSet.getString(1).contains("0000-00-00 00:00:00"));
          assertFalse(resultSet.wasNull());
        }
      }

      ResultSet resultSet = statement.executeQuery("SELECT * from zeroTimestamp");
      assertTrue(resultSet.next());
      assertEquals(null, resultSet.getDate(1));
      assertTrue(resultSet.getString(1).contains("0000-00-00 00:00:00"));
    }
  }

  /**
   * CONJ-405 : Calendar instance not cleared before being used in ResultSet.getTimestamp.
   *
   * @throws SQLException if error
   */
  @Test
  public void clearCalendar() throws SQLException {

    try (Connection connection = setConnection("&useLegacyDatetimeCode=false&serverTimezone=UTC")) {

      try (Statement statement = connection.createStatement()) {
        try (ResultSet resultSet = statement.executeQuery(
            "SELECT '" + TIMESTAMP_1 + "', '" + TIMESTAMP_YEAR_ZERO + "', '" + TIMESTAMP_1 + "'")) {
          testResults(resultSet);
        }
      }

      try (PreparedStatement preparedStatement = connection.prepareStatement(
          "SELECT STR_TO_DATE('" + TIMESTAMP_1 + "', '%Y-%m-%d %H:%i:%s'), "
              + "STR_TO_DATE('" + TIMESTAMP_YEAR_ZERO + "', '%Y-%m-%d %H:%i:%s'), "
              + "STR_TO_DATE('" + TIMESTAMP_1 + "', '%Y-%m-%d %H:%i:%s')")) {
        testResults(preparedStatement.executeQuery());
      }
    }
  }

  private void testResults(ResultSet resultSet) throws SQLException {
    resultSet.next();
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    Timestamp timestamp1 = resultSet.getTimestamp(1, calendar);
    Date date1 = resultSet.getDate(1, calendar);
    resultSet.getTimestamp(2, calendar);

    Timestamp timestamp3 = resultSet.getTimestamp(3, calendar);
    Date date3 = resultSet.getDate(3, calendar);

    assertEquals(date1.getTime(), date3.getTime());
    assertEquals(timestamp1.getTime(), timestamp3.getTime());

  }
}
