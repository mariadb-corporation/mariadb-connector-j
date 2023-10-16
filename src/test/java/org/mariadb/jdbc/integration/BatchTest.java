// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.Calendar;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;

public class BatchTest extends Common {

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    after2();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE BatchTest (t1 int not null primary key auto_increment, t2 LONGTEXT)");
    createSequenceTables();
    stmt.execute("CREATE TABLE timestampCal(id int, val TIMESTAMP)");
  }

  @AfterAll
  public static void after2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS timestampCal");
    stmt.execute("DROP TABLE IF EXISTS BatchTest");
  }

  @Test
  public void wrongParameter() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      wrongParameter(con);
    }
    try (Connection con = createCon("&useServerPrepStmts=true")) {
      wrongParameter(con);
    }
  }

  public void wrongParameter(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE BatchTest");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO BatchTest(t1, t2) VALUES (?,?)")) {
      prep.setInt(1, 5);
      try {
        prep.addBatch();
      } catch (SQLTransientConnectionException e) {
        assertTrue(e.getMessage().contains("Parameter at position 2 is not set"));
      }
      try {
        prep.addBatch();
      } catch (SQLTransientConnectionException e) {
        assertTrue(
            e.getMessage().contains("Parameter at position 2 is not set")
                || e.getMessage()
                    .contains(
                        "batch set of parameters differ from previous set. All parameters must be set"));
      }

      prep.setInt(1, 5);
      prep.setString(3, "wrong position");
      Common.assertThrowsContains(
          SQLTransientConnectionException.class,
          prep::addBatch,
          "Parameter at position 2 is not set");

      prep.setInt(1, 5);
      prep.setString(2, "ok");
      prep.addBatch();
      prep.setString(2, "without position 1");
      prep.addBatch();
    }
  }

  @Test
  public void differentParameterType() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false&useBulkStmtsForInserts=false")) {
      differentParameterType(con, false);
    }
    try (Connection con =
        createCon("&useServerPrepStmts=false&useBulkStmts&useBulkStmtsForInserts")) {
      differentParameterType(con, isMariaDBServer() && !isXpand());
    }
    try (Connection con =
        createCon(
            "&useServerPrepStmts=false&useBulkStmtsForInserts&useBulkStmts&disablePipeline")) {
      differentParameterType(con, isMariaDBServer() && !isXpand());
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmtsForInserts=false")) {
      differentParameterType(con, false);
    }
    try (Connection con =
        createCon("&useServerPrepStmts&useBulkStmtsForInserts&allowLocalInfile=false")) {
      differentParameterType(con, false);
    }
    try (Connection con =
        createCon(
            "&useServerPrepStmts&useBulkStmts&useBulkStmtsForInserts&allowLocalInfile=false")) {
      differentParameterType(con, isMariaDBServer() && !isXpand());
    }
    try (Connection con = createCon("&useServerPrepStmts=false&useBulkStmts&allowLocalInfile")) {
      differentParameterType(con, isMariaDBServer() && !isXpand());
    }
    try (Connection con = createCon("&useServerPrepStmts=false&allowLocalInfile")) {
      differentParameterType(con, false);
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmtsForInserts=false")) {
      differentParameterType(con, false);
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmtsForInserts")) {
      differentParameterType(con, false);
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmts&useBulkStmtsForInserts")) {
      differentParameterType(con, isMariaDBServer() && !isXpand());
    }
    try (Connection con =
        createCon(
            "&useServerPrepStmts&useBulkStmts&useBulkStmtsForInserts&allowLocalInfile=false")) {
      differentParameterType(con, isMariaDBServer() && !isXpand());
    }
    try (Connection con =
        createCon("&useServerPrepStmts&useBulkStmtsForInserts&allowLocalInfile=false")) {
      differentParameterType(con, false);
    }
    try (Connection con =
        createCon("&useServerPrepStmts&useBulkStmtsForInserts=false&disablePipeline=true")) {
      differentParameterType(con, false);
    }
  }

  public void differentParameterType(Connection con, boolean expectSuccessUnknown)
      throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE BatchTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO BatchTest(t1, t2) VALUES (?,?)")) {
      prep.setInt(1, 1);
      prep.setString(2, "1");
      prep.addBatch();

      prep.setInt(1, 2);
      prep.setInt(2, 2);
      prep.addBatch();
      prep.setInt(1, 3);
      prep.setNull(2, Types.INTEGER);
      prep.addBatch();
      int[] res = prep.executeBatch();
      assertEquals(3, res.length);
      assertEquals(1, res[0]);
      assertEquals(1, res[1]);
      assertEquals(1, res[2]);
    }
    ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTest");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("1", rs.getString(2));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals("2", rs.getString(2));
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertNull(rs.getString(2));
    assertFalse(rs.next());
    stmt.execute("TRUNCATE BatchTest");

    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO BatchTest(t1, t2) VALUES (?,?)")) {
      prep.setInt(1, 1);
      prep.setInt(2, 1);
      prep.addBatch();

      prep.setInt(1, 2);
      prep.setInt(2, 2);
      prep.addBatch();
      int[] res = prep.executeBatch();
      assertEquals(2, res.length);
      assertEquals(1, res[0]);
      assertEquals(1, res[1]);
    }
    rs = stmt.executeQuery("SELECT * FROM BatchTest");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("1", rs.getString(2));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals("2", rs.getString(2));
    assertFalse(rs.next());

    stmt.execute("TRUNCATE BatchTest");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO BatchTest(t1, t2) VALUES (?,?)")) {
      prep.setInt(1, 1);
      prep.setInt(2, 1);
      prep.addBatch();

      int[] res = prep.executeBatch();
      assertEquals(1, res.length);
      assertEquals(1, res[0]);
    }
    rs = stmt.executeQuery("SELECT * FROM BatchTest");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("1", rs.getString(2));
    assertFalse(rs.next());

    stmt.execute("TRUNCATE BatchTest");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO BatchTest(t1, t2) VALUES (?,?)")) {
      prep.setInt(1, 1);
      prep.setString(2, "1");
      prep.addBatch();

      prep.setInt(1, 2);
      prep.setInt(2, 2);
      prep.addBatch();
      int[] res = prep.executeBatch();
      assertEquals(2, res.length);
      assertEquals(1, res[0]);
      assertEquals(1, res[1]);

      stmt.execute("TRUNCATE BatchTest");

      stmt.setFetchSize(1);
      rs = stmt.executeQuery("SELECT * FROM sequence_1_to_10");
      rs.next();

      prep.setInt(1, 1);
      prep.setString(2, "1");
      prep.addBatch();

      prep.setInt(1, 2);
      prep.setInt(2, 2);
      prep.addBatch();
      res = prep.executeBatch();
      assertEquals(2, res.length);
      assertEquals(1, res[0]);
      assertEquals(1, res[1]);
    }
    rs = stmt.executeQuery("SELECT * FROM BatchTest");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("1", rs.getString(2));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals("2", rs.getString(2));
    assertFalse(rs.next());

    try (PreparedStatement prep =
        con.prepareStatement("UPDATE BatchTest SET t1=t1+10 WHERE t1=?")) {
      prep.setInt(1, 1);
      prep.addBatch();

      prep.setInt(1, 2);
      prep.addBatch();
      int[] res = prep.executeBatch();
      if (expectSuccessUnknown) {
        assertEquals(Statement.SUCCESS_NO_INFO, res[0]);
        assertEquals(Statement.SUCCESS_NO_INFO, res[1]);
      } else {
        assertEquals(1, res[0]);
        assertEquals(1, res[1]);
      }
    }
    con.rollback();
  }

  @Test
  public void largeBatch() throws SQLException {
    for (int i = 0; i < 64; i++) {
      boolean useServerPrepStmts = (i & 2) > 0;
      boolean useBulkStmts = (i & 4) > 0;
      boolean allowLocalInfile = (i & 8) > 0;
      boolean useCompression = (i & 16) > 0;
      boolean useBulkStmtsForInserts = (i & 32) > 0;

      String confString =
          String.format(
              "&useServerPrepStmts=%s&useBulkStmts=%s&allowLocalInfile=%s&useCompression=%s&useBulkStmtsForInserts=%s",
              useServerPrepStmts,
              useBulkStmts,
              allowLocalInfile,
              useCompression,
              useBulkStmtsForInserts);
      try (Connection con = createCon(confString)) {
        largeBatch(con);
      }
    }
  }

  public void largeBatch(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE BatchTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO BatchTest(t1, t2) VALUES (?,?)")) {
      prep.setInt(1, 1);
      prep.setString(2, "1");
      prep.addBatch();

      prep.setInt(1, 2);
      prep.setInt(2, 2);
      prep.addBatch();
      long[] res = prep.executeLargeBatch();
      assertEquals(2, res.length);
      assertEquals(1, res[0]);
      assertEquals(1, res[1]);
    }
    ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTest");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("1", rs.getString(2));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals("2", rs.getString(2));
    assertFalse(rs.next());
    con.commit();
  }

  @Test
  public void bulkPacketSplitMaxAllowedPacket() throws SQLException {
    Assumptions.assumeTrue(runLongTest());
    int maxAllowedPacket = getMaxAllowedPacket();
    bulkPacketSplit(2, maxAllowedPacket - 40, maxAllowedPacket);
    if (maxAllowedPacket >= 16 * 1024 * 1024) bulkPacketSplit(2, maxAllowedPacket - 40, null);
  }

  @Test
  public void bulkPacketSplitMultiplePacket() throws SQLException {
    Assumptions.assumeTrue(runLongTest());
    int maxAllowedPacket = getMaxAllowedPacket();
    bulkPacketSplit(4, getMaxAllowedPacket() / 3, maxAllowedPacket);
    if (maxAllowedPacket >= 16 * 1024 * 1024) bulkPacketSplit(4, getMaxAllowedPacket() / 3, null);
  }

  @Test
  public void bulkPacketSplitHugeNbPacket() throws SQLException {
    Assumptions.assumeTrue(runLongTest());
    int maxAllowedPacket = getMaxAllowedPacket();
    bulkPacketSplit(getMaxAllowedPacket() / 8000, 20, maxAllowedPacket);
    if (maxAllowedPacket >= 16 * 1024 * 1024)
      bulkPacketSplit(getMaxAllowedPacket() / 8000, 20, null);
  }

  public void bulkPacketSplit(int nb, int len, Integer maxAllowedPacket) throws SQLException {
    byte[] arr = new byte[Math.min(16 * 1024 * 1024, len)];
    for (int pos = 0; pos < arr.length; pos++) {
      arr[pos] = (byte) ((pos % 60) + 65);
    }

    try (Connection con =
        createCon(
            "&useServerPrepStmts&useBulkStmts"
                + (maxAllowedPacket != null ? "&maxAllowedPacket=" + maxAllowedPacket : ""))) {
      Statement stmt = con.createStatement();
      stmt.execute("TRUNCATE BatchTest");
      stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
      try (PreparedStatement prep =
          con.prepareStatement("INSERT INTO BatchTest(t1, t2) VALUES (?,?)")) {
        for (int i = 1; i <= nb; i++) {
          prep.setInt(1, i);
          prep.setBytes(2, arr);
          prep.addBatch();
        }

        int[] res = prep.executeBatch();
        assertEquals(nb, res.length);
        for (int i = 0; i < nb; i++) {
          assertTrue(res[i] == 1 || res[i] == Statement.SUCCESS_NO_INFO);
        }
      }
      ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTest");
      for (int i = 1; i <= nb; i++) {
        assertTrue(rs.next());
        assertEquals(i, rs.getInt(1));
        assertArrayEquals(arr, rs.getBytes(2));
      }
      assertFalse(rs.next());

      // check same ending with error
      stmt.execute("TRUNCATE BatchTest");
      try (PreparedStatement prep =
          con.prepareStatement("INSERT INTO BatchTest(t1, t2) VALUES (?,?)")) {
        for (int i = 1; i <= nb; i++) {
          prep.setInt(1, i);
          prep.setBytes(2, arr);
          prep.addBatch();
        }
        prep.setInt(1, nb);
        prep.setBytes(2, arr);
        prep.addBatch();

        BatchUpdateException e =
            Assertions.assertThrows(BatchUpdateException.class, prep::executeBatch);
        int[] updateCounts = e.getUpdateCounts();
        assertEquals(nb + 1, updateCounts.length);
      }
      con.rollback();
      con.rollback();
    }
  }

  @Test
  public void batchWithError() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false&useBulkStmts=false")) {
      batchWithError(con);
    }
    try (Connection con = createCon("&useServerPrepStmts=false&useBulkStmts=true")) {
      batchWithError(con);
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmts=false")) {
      batchWithError(con);
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmts=true")) {
      batchWithError(con);
    }
    try (Connection con =
        createCon("&useServerPrepStmts=false&useBulkStmts=false&allowLocalInfile")) {
      batchWithError(con);
    }
    try (Connection con =
        createCon("&useServerPrepStmts=false&useBulkStmts=true&allowLocalInfile")) {
      batchWithError(con);
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmts=false&allowLocalInfile")) {
      batchWithError(con);
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmts=true&allowLocalInfile")) {
      batchWithError(con);
    }
  }

  private void batchWithError(Connection con) throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer());
    Statement stmt = con.createStatement();
    stmt.execute("DROP TABLE IF EXISTS prepareError");
    stmt.setFetchSize(3);
    stmt.execute("CREATE TABLE prepareError(id int primary key, val varchar(10))");
    stmt.execute("INSERT INTO prepareError(id, val) values (1, 'val1')");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO prepareError(id, val) VALUES (?,?)")) {
      prep.setInt(1, 1);
      prep.setString(2, "val3");
      prep.addBatch();
      // Duplicate entry '1' for key 'PRIMARY'
      assertThrows(BatchUpdateException.class, prep::executeBatch);
    }
  }

  @Test
  public void ensureCalendarSync() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer() && !isXpand());
    // to ensure that calendar is use at the same time, using BULK command
    TimestampCal[] t1 = new TimestampCal[50];
    for (int i = 0; i < 50; i++) {
      t1[i] = new TimestampCal(Timestamp.valueOf((1970 + i) + "-01-31 12:00:00.0"), i);
    }
    TimestampCal[] t2 = new TimestampCal[50];
    for (int i = 0; i < 50; i++) {
      t2[i] = new TimestampCal(Timestamp.valueOf((1970 + i) + "-12-01 01:12:15.0"), i + 50);
    }

    Calendar cal = Calendar.getInstance();
    sharedConn.createStatement().execute("START TRANSACTION");
    int inserts = Stream.of(t1, t2).parallel().mapToInt(l -> insertTimestamp(l, cal)).sum();
    assertEquals(100, inserts);
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM timestampCal order by ID");
    for (int i = 0; i < 50; i++) {
      rs.next();
      assertEquals(t1[i].getVal().toString(), rs.getTimestamp(2, cal).toString());
    }
    for (int i = 0; i < 50; i++) {
      rs.next();
      assertEquals(t2[i].getVal().toString(), rs.getTimestamp(2, cal).toString());
    }
    sharedConn.commit();
  }

  private int insertTimestamp(TimestampCal[] vals, Calendar cal) {
    try (Connection con = createCon()) {
      try (PreparedStatement prep =
          con.prepareStatement("INSERT INTO timestampCal(val, id) VALUES (?,?)")) {
        for (int i = 0; i < vals.length; i++) {
          prep.setTimestamp(1, vals[i].getVal(), cal);
          prep.setInt(2, vals[i].getId());
          prep.addBatch();
        }
        return prep.executeBatch().length;
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return -1;
    }
  }

  private class TimestampCal {
    private final Timestamp val;
    private final int id;

    public TimestampCal(Timestamp val, int id) {
      this.val = val;
      this.id = id;
    }

    public Timestamp getVal() {
      return val;
    }

    public int getId() {
      return id;
    }

    @Override
    public String toString() {
      return "TimestampCal{" + "val=" + val + ", id=" + id + '}';
    }
  }
}
