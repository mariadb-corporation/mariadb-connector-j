// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;

public class FloatArrayCodecTest extends CommonCodecTest {
  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS BinaryCodec");
    stmt.execute("DROP TABLE IF EXISTS BinaryCodec2");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE BinaryCodec (t0 int not null primary key auto_increment, t1 blob) CHARACTER"
            + " SET utf8mb4 COLLATE utf8mb4_unicode_ci");
    stmt.execute("FLUSH TABLES");
  }

  @Test
  public void floatArray() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE TABLE BinaryCodec");
    floatArray(sharedConn);
    floatArray(sharedConnBinary);
  }

  private void floatArray(Connection con) throws SQLException {
    float[] val = new float[] {1, 2, 3};
    byte[] expectedConverstion =
        new byte[] {0x00, 0x00, (byte) 0x80, 0x3F, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40};
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO BinaryCodec(t1) VALUES (?)")) {
      prep.setObject(1, val);
      prep.execute();
    }
    try (PreparedStatement prep = con.prepareStatement("SELECT * FROM BinaryCodec")) {
      ResultSet rs = prep.executeQuery();
      assertTrue(rs.next());
      assertArrayEquals(expectedConverstion, rs.getBytes(2));
      float[] res = rs.getObject(2, float[].class);
      assertArrayEquals(val, res);
    }
  }

  @Test
  public void floatArrayArrayObj() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE TABLE BinaryCodec");
    floatArrayArrayObj(sharedConn);
    stmt.execute("TRUNCATE TABLE BinaryCodec");
    floatArrayArrayObj(sharedConnBinary);
  }

  private void floatArrayArrayObj(org.mariadb.jdbc.Connection con) throws SQLException {
    float[] val = new float[] {1, 2, 3};
    float[] val2 = new float[] {4, 5, 6};

    Array valArray = con.createArrayOf("float", val);
    Array valArray2 = con.createArrayOf("float", val2);
    byte[] expectedConverstion =
        new byte[] {0x00, 0x00, (byte) 0x80, 0x3F, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40};
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO BinaryCodec(t0, t1) VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setArray(2, valArray);
      prep.execute();
    }
    try (PreparedStatement prep =
        con.prepareStatement(
            "SELECT * FROM BinaryCodec",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = prep.executeQuery();
      assertTrue(rs.next());
      assertArrayEquals(expectedConverstion, rs.getBytes(2));
      float[] res = rs.getObject(2, float[].class);
      assertArrayEquals(val, res);
      Array resArray = rs.getArray(2);
      assertEquals(valArray, resArray);
      rs.updateArray(2, valArray2);
      rs.updateRow();
    }
    try (PreparedStatement prep =
        con.prepareStatement(
            "SELECT * FROM BinaryCodec",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = prep.executeQuery();
      assertTrue(rs.next());
      float[] res = rs.getObject(2, float[].class);
      assertArrayEquals(val2, res);
      Array resArray = rs.getArray(2);
      assertEquals(valArray2, resArray);
    }
  }

  @Test
  public void floatArrayObjArray() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE TABLE BinaryCodec");
    floatArrayObjArray(sharedConn);
    floatArrayObjArray(sharedConnBinary);
  }

  private void floatArrayObjArray(org.mariadb.jdbc.Connection con) throws SQLException {
    Float[] val = new Float[] {1f, 2f, 3f};
    Array valArray = con.createArrayOf("Float", val);
    byte[] expectedConverstion =
        new byte[] {0x00, 0x00, (byte) 0x80, 0x3F, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40};
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO BinaryCodec(t1) VALUES (?)")) {
      prep.setArray(1, valArray);
      prep.execute();
    }
    try (PreparedStatement prep = con.prepareStatement("SELECT * FROM BinaryCodec")) {
      ResultSet rs = prep.executeQuery();
      assertTrue(rs.next());
      assertArrayEquals(expectedConverstion, rs.getBytes(2));
      Float[] res = rs.getObject(2, Float[].class);
      assertArrayEquals(val, res);
      Array resArray = rs.getArray(2);
      assertEquals(valArray, resArray);
    }
  }

  @Test
  public void floatObjectArray() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE TABLE BinaryCodec");
    floatObjectArray(sharedConn);
    floatObjectArray(sharedConnBinary);
  }

  private void floatObjectArray(Connection con) throws SQLException {
    Float[] val = new Float[] {1f, 2f, 3f};
    byte[] expectedConverstion =
        new byte[] {0x00, 0x00, (byte) 0x80, 0x3F, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x40, 0x40};
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO BinaryCodec(t1) VALUES (?)")) {
      prep.setObject(1, val);
      prep.execute();
    }
    try (PreparedStatement prep = con.prepareStatement("SELECT * FROM BinaryCodec")) {
      ResultSet rs = prep.executeQuery();
      assertTrue(rs.next());
      assertArrayEquals(expectedConverstion, rs.getBytes(2));
      Float[] res = rs.getObject(2, Float[].class);
      assertArrayEquals(val, res);
    }
  }
}
