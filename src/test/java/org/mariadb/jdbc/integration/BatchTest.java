/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
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
 */

package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Common;
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
      wrongParameter(con, true);
    }
    try (Connection con = createCon("&useServerPrepStmts=true")) {
      wrongParameter(con, false);
    }
  }

  public void wrongParameter(Connection con, boolean text) throws SQLException {
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
      assertThrowsContains(
          SQLTransientConnectionException.class,
          () -> prep.addBatch(),
          "Parameter at position 2 is not set");

      prep.setInt(1, 5);
      prep.setString(2, "ok");
      prep.addBatch();
      prep.setString(2, "without position 1");
      assertThrowsContains(
          SQLTransientConnectionException.class,
          () -> prep.addBatch(),
          "Parameter at " + "position 1 is not set");
    }
  }

  @Test
  public void differentParameterType() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      differentParameterType(con);
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmts=false")) {
      differentParameterType(con);
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmts")) {
      differentParameterType(con);
    }
  }

  public void differentParameterType(Connection con) throws SQLException {
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
  }

  @Test
  public void largeBatch() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      largeBatch(con);
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmts=false")) {
      largeBatch(con);
    }
    try (Connection con = createCon("&useServerPrepStmts&useBulkStmts")) {
      largeBatch(con);
    }
  }

  public void largeBatch(Connection con) throws SQLException {
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
  }

  @Test
  public void bulkPacketSplitMaxAllowedPacket() throws SQLException {
    bulkPacketSplit(2, getMaxAllowedPacket() - 40);
  }

  @Test
  public void bulkPacketSplitMultiplePacket() throws SQLException {
    bulkPacketSplit(getMaxAllowedPacket() / 800, 1000);
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
            Assertions.assertThrows(BatchUpdateException.class, () -> prep.executeBatch());
        int[] updateCounts = e.getUpdateCounts();
        assertEquals(nb + 1, updateCounts.length);
      }
    }
  }

  private int getMaxAllowedPacket() throws SQLException {
    java.sql.Statement st = sharedConn.createStatement();
    ResultSet rs = st.executeQuery("select @@max_allowed_packet");
    assertTrue(rs.next());
    return rs.getInt(1);
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
      assertThrowsContains(
          BatchUpdateException.class,
          () -> prep.executeBatch(),
          "Duplicate entry '1' for key 'PRIMARY'");
    }
  }
}
