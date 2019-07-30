package org.mariadb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.junit.Test;

public class LocalTimeTest extends BaseTest {

  @Test
  public void localTimeTest() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    stmt.execute("CREATE TEMPORARY TABLE LocalTimeTest(val TIME(6), val2 TIME)");
    try (PreparedStatement prep =
        sharedConnection.prepareStatement("INSERT INTO LocalTimeTest value (?, ?)")) {
      prep.setObject(1, LocalTime.of(1, 2, 3, 123456789));
      prep.setObject(2, LocalTime.of(2, 3, 4, 120000000));
      prep.execute();

      prep.setObject(1, LocalTime.of(15, 14, 13, 12340000));
      prep.setObject(2, LocalTime.of(16, 15, 14, 12340000));
      prep.execute();

      prep.setObject(1, LocalTime.of(5, 4, 3, 0));
      prep.setObject(2, LocalTime.of(6, 5, 4, 0));
      prep.execute();
    }
    ResultSet rs = stmt.executeQuery("SELECT * FROM LocalTimeTest");
    assertTrue(rs.next());

    assertEquals("01:02:03.123456", rs.getString(1));
    assertEquals(LocalTime.of(1, 2, 3, 123456000), rs.getObject(1, LocalTime.class));
    assertEquals("01:02:03", rs.getTime(1).toString());
    assertEquals(Time.valueOf("01:02:03").getTime() + 123, rs.getTime(1).getTime());

    assertEquals("02:03:04", rs.getString(2));
    assertEquals(LocalTime.of(2, 3, 4, 0), rs.getObject(2, LocalTime.class));
    assertEquals(Time.valueOf("02:03:04"), rs.getTime(2));
    assertEquals(Time.valueOf("02:03:04").getTime(), rs.getTime(2).getTime());

    assertTrue(rs.next());

    assertEquals("15:14:13.012340", rs.getString(1));
    assertEquals(LocalTime.of(15, 14, 13, 12340000), rs.getObject(1, LocalTime.class));
    assertEquals(Time.valueOf("15:14:13").toString(), rs.getTime(1).toString());
    assertEquals(Time.valueOf("15:14:13").getTime() + 12, rs.getTime(1).getTime());

    assertEquals("16:15:14", rs.getString(2));
    assertEquals(LocalTime.of(16, 15, 14, 0), rs.getObject(2, LocalTime.class));
    assertEquals(Time.valueOf("16:15:14").toString(), rs.getTime(2).toString());
    assertEquals(Time.valueOf("16:15:14").getTime(), rs.getTime(2).getTime());

    assertTrue(rs.next());

    assertEquals("05:04:03.000000", rs.getString(1));
    assertEquals(LocalTime.of(5, 4, 3, 0), rs.getObject(1, LocalTime.class));
    assertEquals(Time.valueOf("05:04:03").toString(), rs.getTime(1).toString());
    assertEquals(Time.valueOf("05:04:03").getTime(), rs.getTime(1).getTime());

    assertEquals("06:05:04", rs.getString(2));
    assertEquals(LocalTime.of(6, 5, 4, 0), rs.getObject(2, LocalTime.class));
    assertEquals(Time.valueOf("06:05:04").toString(), rs.getTime(2).toString());
    assertEquals(Time.valueOf("06:05:04").getTime(), rs.getTime(2).getTime());

    assertFalse(rs.next());
  }

  @Test
  public void localDateTest() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    stmt.execute("CREATE TEMPORARY TABLE LocalDateTest(val DATE)");
    try (PreparedStatement prep =
        sharedConnection.prepareStatement("INSERT INTO LocalDateTest value (?)")) {
      prep.setObject(1, LocalDate.of(2000, 12, 31));
      prep.execute();
      prep.setObject(1, LocalDate.of(1000, 1, 1));
      prep.execute();
      prep.setObject(1, LocalDate.of(9999, 1, 31));
      prep.execute();
    }
    ResultSet rs = stmt.executeQuery("SELECT * FROM LocalDateTest");
    assertTrue(rs.next());

    assertEquals("2000-12-31", rs.getString(1));
    assertEquals(LocalDate.of(2000, 12, 31), rs.getObject(1, LocalDate.class));

    assertTrue(rs.next());
    assertEquals("1000-01-01", rs.getString(1));
    assertEquals(LocalDate.of(1000, 1, 1), rs.getObject(1, LocalDate.class));

    assertTrue(rs.next());
    assertEquals("9999-01-31", rs.getString(1));
    assertEquals(LocalDate.of(9999, 1, 31), rs.getObject(1, LocalDate.class));
    assertFalse(rs.next());
  }

  @Test
  public void localDateTimeTest() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    stmt.execute("CREATE TEMPORARY TABLE LocalDateTimeTest(val DATETIME(6), val2 DATETIME)");
    try (PreparedStatement prep =
        sharedConnection.prepareStatement("INSERT INTO LocalDateTimeTest value (?, ?)")) {
      prep.setObject(1, LocalDateTime.of(2000, 12, 31, 1, 2, 3, 123456789));
      prep.setObject(2, LocalDateTime.of(2000, 12, 31, 2, 3, 4, 120000000));
      prep.execute();

      prep.setObject(1, LocalDateTime.of(1000, 1, 1, 15, 14, 13, 12340000));
      prep.setObject(2, LocalDateTime.of(1000, 1, 1, 16, 15, 14, 12340000));
      prep.execute();

      prep.setObject(1, LocalDateTime.of(9999, 12, 31, 5, 4, 3, 0));
      prep.setObject(2, LocalDateTime.of(9999, 12, 31, 6, 5, 4, 0));
      prep.execute();
    }
    ResultSet rs = stmt.executeQuery("SELECT * FROM LocalDateTimeTest");
    assertTrue(rs.next());

    assertEquals("2000-12-31 01:02:03.123456", rs.getString(1));
    assertEquals(
        LocalDateTime.of(2000, 12, 31, 1, 2, 3, 123456000), rs.getObject(1, LocalDateTime.class));
    assertEquals("2000-12-31 01:02:03.123456", rs.getTimestamp(1).toString());
    assertEquals(
        Timestamp.valueOf("2000-12-31 01:02:03").getTime() + 123, rs.getTimestamp(1).getTime());
    assertEquals(LocalTime.of(1, 2, 3, 123456000), rs.getObject(1, LocalTime.class));
    assertEquals("01:02:03", rs.getTime(1).toString());
    assertEquals(Timestamp.valueOf("2000-12-31 01:02:03").getTime() + 123, rs.getTime(1).getTime());
    assertEquals(LocalDate.of(2000, 12, 31), rs.getObject(1, LocalDate.class));

    assertEquals("2000-12-31 02:03:04.0", rs.getString(2));
    assertEquals(LocalDateTime.of(2000, 12, 31, 2, 3, 4, 0), rs.getObject(2, LocalDateTime.class));
    assertEquals(Timestamp.valueOf("2000-12-31 02:03:04"), rs.getTimestamp(2));
    assertEquals(Timestamp.valueOf("2000-12-31 02:03:04").getTime(), rs.getTimestamp(2).getTime());
    assertEquals(LocalTime.of(2, 3, 4, 0), rs.getObject(2, LocalTime.class));
    assertEquals(Time.valueOf("02:03:04").toString(), rs.getTime(2).toString());
    assertEquals(Timestamp.valueOf("2000-12-31 02:03:04").getTime(), rs.getTime(2).getTime());
    assertEquals(LocalDate.of(2000, 12, 31), rs.getObject(2, LocalDate.class));

    assertTrue(rs.next());

    assertEquals("1000-01-01 15:14:13.01234", rs.getString(1));
    assertEquals(
        LocalDateTime.of(1000, 1, 1, 15, 14, 13, 12340000), rs.getObject(1, LocalDateTime.class));
    assertEquals(
        Timestamp.valueOf("1000-01-01 15:14:13.01234").toString(), rs.getTimestamp(1).toString());
    assertEquals(
        Timestamp.valueOf("1000-01-01 15:14:13").getTime() + 12, rs.getTimestamp(1).getTime());
    assertEquals(LocalTime.of(15, 14, 13, 12340000), rs.getObject(1, LocalTime.class));
    assertEquals(Time.valueOf("15:14:13").toString(), rs.getTime(1).toString());
    assertEquals(Timestamp.valueOf("1000-01-01 15:14:13.01234").getTime(), rs.getTime(1).getTime());
    assertEquals(LocalDate.of(1000, 1, 1), rs.getObject(1, LocalDate.class));

    assertEquals("1000-01-01 16:15:14.0", rs.getString(2));
    assertEquals(LocalDateTime.of(1000, 1, 1, 16, 15, 14, 0), rs.getObject(2, LocalDateTime.class));
    assertEquals(
        Timestamp.valueOf("1000-01-01 16:15:14").toString(), rs.getTimestamp(2).toString());
    assertEquals(Timestamp.valueOf("1000-01-01 16:15:14").getTime(), rs.getTimestamp(2).getTime());
    assertEquals(LocalTime.of(16, 15, 14, 0), rs.getObject(2, LocalTime.class));
    assertEquals(Time.valueOf("16:15:14").toString(), rs.getTime(2).toString());
    assertEquals(Timestamp.valueOf("1000-01-01 16:15:14").getTime(), rs.getTime(2).getTime());
    assertEquals(LocalDate.of(1000, 1, 1), rs.getObject(2, LocalDate.class));

    assertTrue(rs.next());

    assertEquals("9999-12-31 05:04:03.0", rs.getString(1));
    assertEquals(LocalDateTime.of(9999, 12, 31, 5, 4, 3, 0), rs.getObject(1, LocalDateTime.class));
    assertEquals(
        Timestamp.valueOf("9999-12-31 05:04:03").toString(), rs.getTimestamp(1).toString());
    assertEquals(Timestamp.valueOf("9999-12-31 05:04:03").getTime(), rs.getTimestamp(1).getTime());
    assertEquals(LocalTime.of(5, 4, 3, 0), rs.getObject(1, LocalTime.class));
    assertEquals(Time.valueOf("05:04:03").toString(), rs.getTime(1).toString());
    assertEquals(Timestamp.valueOf("9999-12-31 05:04:03").getTime(), rs.getTime(1).getTime());
    assertEquals(LocalDate.of(9999, 12, 31), rs.getObject(1, LocalDate.class));

    assertEquals("9999-12-31 06:05:04.0", rs.getString(2));
    assertEquals(LocalDateTime.of(9999, 12, 31, 6, 5, 4, 0), rs.getObject(2, LocalDateTime.class));
    assertEquals(
        Timestamp.valueOf("9999-12-31 06:05:04").toString(), rs.getTimestamp(2).toString());
    assertEquals(Timestamp.valueOf("9999-12-31 06:05:04").getTime(), rs.getTimestamp(2).getTime());
    assertEquals(LocalTime.of(6, 5, 4, 0), rs.getObject(2, LocalTime.class));
    assertEquals(Time.valueOf("06:05:04").toString(), rs.getTime(2).toString());
    assertEquals(Timestamp.valueOf("9999-12-31 06:05:04").getTime(), rs.getTime(2).getTime());
    assertEquals(LocalDate.of(9999, 12, 31), rs.getObject(1, LocalDate.class));

    assertFalse(rs.next());
  }
}
