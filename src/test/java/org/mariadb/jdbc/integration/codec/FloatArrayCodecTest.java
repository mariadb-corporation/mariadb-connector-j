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
        "CREATE TABLE BinaryCodec (t1 blob) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
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
      assertArrayEquals(expectedConverstion, rs.getBytes(1));
      float[] res = rs.getObject(1, float[].class);
      assertArrayEquals(val, res);
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
      assertArrayEquals(expectedConverstion, rs.getBytes(1));
      Float[] res = rs.getObject(1, Float[].class);
      assertArrayEquals(val, res);
    }
  }
}
