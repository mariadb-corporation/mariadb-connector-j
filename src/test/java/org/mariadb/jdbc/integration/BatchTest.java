// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;

public class BatchTest extends Common {

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS BatchTest");
    stmt.execute(
        "CREATE TABLE BatchTest (t1 int not null primary key auto_increment, t2 LONGTEXT)");
  }

  @AfterAll
  public static void after2() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE BatchTest");
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
      Common.assertThrowsContains(
          SQLTransientConnectionException.class,
          prep::addBatch,
          "Parameter at " + "position 1 is not set");
    }
  }

  @Test
  public void differentParameterType() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false&useBulkStmts=false")) {
      differentParameterType(con, false);
    }
    try (Connection con = createCon("&useServerPrepStmts=false&useBulkStmts=true")) {
      differentParameterType(con, isMariaDBServer());
    }
    try (Connection con =
        createCon("&useServerPrepStmts=false&useBulkStmts=true&disablePipeline")) {
      differentParameterType(con, isMariaDBServer());
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmts=false")) {
      differentParameterType(con, false);
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmts")) {
      differentParameterType(con, isMariaDBServer());
    }
    try (Connection con = createCon("&useServerPrepStmts=false&allowLocalInfile")) {
      differentParameterType(con, isMariaDBServer());
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmts=false&allowLocalInfile")) {
      differentParameterType(con, false);
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmts&allowLocalInfile")) {
      differentParameterType(con, false);
    }
    try (Connection con =
        createCon("&useServerPrepStmts&useBulkStmts=false&disablePipeline=true")) {
      differentParameterType(con, false);
    }
  }

  public void differentParameterType(Connection con, boolean expectSuccessUnknown)
      throws SQLException {
    Statement stmt = con.createStatement();
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
    }
    ResultSet rs = stmt.executeQuery("SELECT * FROM BatchTest");
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

      prep.setInt(1, 2);
      prep.setInt(2, 2);
      prep.addBatch();
      int[] res = prep.executeBatch();
      assertEquals(2, res.length);
      if (expectSuccessUnknown) {
        assertEquals(Statement.SUCCESS_NO_INFO, res[0]);
        assertEquals(Statement.SUCCESS_NO_INFO, res[1]);
      } else {
        assertEquals(1, res[0]);
        assertEquals(1, res[1]);
      }
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
      prep.setString(2, "1");
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
  }

  @Test
  public void largeBatch() throws SQLException {
    for (int i = 0; i < 32; i++) {
      boolean useServerPrepStmts = (i & 2) > 0;
      boolean useBulkStmts = (i & 4) > 0;
      boolean allowLocalInfile = (i & 8) > 0;
      boolean useCompression = (i & 16) > 0;

      try (Connection con =
          createCon(
              String.format(
                  "&useServerPrepStmts=%s&useBulkStmts=%s&allowLocalInfile=%s&useCompression=%s",
                  useServerPrepStmts, useBulkStmts, allowLocalInfile, useCompression))) {
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
    int maxAllowedPacket = getMaxAllowedPacket();
    bulkPacketSplit(2, maxAllowedPacket - 40);
  }

  @Test
  public void bulkPacketSplitMultiplePacket() throws SQLException {
    bulkPacketSplit(4, getMaxAllowedPacket() / 3);
  }

  @Test
  public void bulkPacketSplitHugeNbPacket() throws SQLException {
    bulkPacketSplit(getMaxAllowedPacket() / 8000, 20);
  }

  public void bulkPacketSplit(int nb, int len) throws SQLException {
    byte[] arr = new byte[Math.min(16 * 1024 * 1024, len)];
    for (int pos = 0; pos < arr.length; pos++) {
      arr[pos] = (byte) ((pos % 60) + 65);
    }

    try (Connection con = createCon("&useServerPrepStmts&useBulkStmts")) {
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
      Common.assertThrowsContains(
          BatchUpdateException.class, prep::executeBatch, "Duplicate entry '1' for key 'PRIMARY'");
    }
  }
}
