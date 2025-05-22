// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.HashMap;
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
    try (PreparedStatement prep =
        con.prepareStatement(
            "SELECT * FROM BinaryCodec",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)) {
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
  public void floatArrayArrayObjWithType() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE TABLE BinaryCodec");
    floatArrayArrayObjWithType(sharedConn);
    stmt.execute("TRUNCATE TABLE BinaryCodec");
    floatArrayArrayObjWithType(sharedConnBinary);
  }

  private void floatArrayArrayObjWithType(org.mariadb.jdbc.Connection con) throws SQLException {
    float[] val = new float[] {1, 2, 3};
    float[] val2 = new float[] {4, 5};
    float[] val3 = new float[] {7, 8, 9, 10};

    Array valArray = con.createArrayOf("float", val);

    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO BinaryCodec(t0, t1) VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setObject(2, valArray, Types.ARRAY);
      prep.execute();

      prep.setInt(1, 2);
      prep.setObject(2, new Float[] {4f, 5f}, Types.ARRAY);
      prep.execute();

      prep.setInt(1, 3);
      prep.setObject(2, val3, Types.ARRAY);
      prep.execute();
    }

    try (PreparedStatement prep =
        con.prepareStatement(
            "SELECT * FROM BinaryCodec",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = prep.executeQuery();
      assertTrue(rs.next());
      float[] res = (float[]) rs.getArray(2).getArray();
      assertArrayEquals(val, res);
      Array arr = rs.getArray(2);
      assertArrayEquals(val, (float[]) arr.getArray(1, 3));
      assertArrayEquals(new float[] {2, 3}, (float[]) arr.getArray(2, 2));
      assertArrayEquals(new float[] {1, 2}, (float[]) arr.getArray(1, 2));
      assertThrowsContains(
          SQLException.class,
          () -> arr.getArray(0, 2),
          "Wrong index position. Is 0 but must be in 1-3 range");
      assertThrowsContains(
          SQLException.class,
          () -> arr.getArray(2, 20),
          "Count value is too big. Count is 20 but cannot be > to 2");
      assertEquals("float[]", arr.getBaseTypeName());
      assertEquals(Types.FLOAT, arr.getBaseType());
      assertThrowsContains(
          SQLException.class,
          () -> arr.getArray(new HashMap<>()),
          "getArray(Map<String, Class<?>> map) is not supported");
      assertThrowsContains(
          SQLException.class,
          () -> arr.getArray(1, 2, new HashMap<>()),
          "getArray(long index, int count, Map<String, Class<?>> map) is not supported");

      ResultSet rss = arr.getResultSet();
      assertTrue(rss.next());
      assertEquals(1, rss.getFloat(1));
      assertTrue(rss.next());
      assertEquals(2, rss.getFloat(1));
      assertTrue(rss.next());
      assertEquals(3, rss.getFloat(1));
      assertFalse(rss.next());

      rss = arr.getResultSet(2, 2);
      assertTrue(rss.next());
      assertEquals(2, rss.getFloat(1));
      assertTrue(rss.next());
      assertEquals(3, rss.getFloat(1));
      assertFalse(rss.next());
      assertThrowsContains(
          SQLException.class,
          () -> arr.getResultSet(new HashMap<>()),
          "getResultSet(Map<String, Class<?>> map) is not supported");
      assertThrowsContains(
          SQLException.class,
          () -> arr.getResultSet(1, 2, new HashMap<>()),
          "getResultSet(long index, int count, Map<String, Class<?>> map) is not supported");
      arr.free();
      assertTrue(rs.next());
      float[] res2 = rs.getObject(2, float[].class);
      assertArrayEquals(val2, res2);

      assertTrue(rs.next());
      float[] res3 = rs.getObject(2, float[].class);
      assertArrayEquals(val3, res3);
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
    try (PreparedStatement prep =
        con.prepareStatement(
            "SELECT * FROM BinaryCodec",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)) {
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
    try (PreparedStatement prep =
        con.prepareStatement(
            "SELECT * FROM BinaryCodec",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)) {
      ResultSet rs = prep.executeQuery();
      assertTrue(rs.next());
      assertArrayEquals(expectedConverstion, rs.getBytes(2));
      Float[] res = rs.getObject(2, Float[].class);
      assertArrayEquals(val, res);
    }
  }
}
