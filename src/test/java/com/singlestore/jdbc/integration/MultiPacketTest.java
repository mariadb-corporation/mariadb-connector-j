// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.singlestore.jdbc.Common;
import com.singlestore.jdbc.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MultiPacketTest extends Common {
  private static final char[] arr2 = new char[17 * 1024 * 1024];

  static {
    for (int pos = 0; pos < arr2.length; pos++) {
      arr2[pos] = (char) ('A' + (pos % 60));
    }
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Assumptions.assumeTrue(getMaxAllowedPacket() > 19 * 1024 * 1024);
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS multiPacketTest");
    stmt.execute("CREATE TABLE multiPacketTest (t1 MEDIUMTEXT, t2 LONGTEXT)");
  }

  @AfterAll
  public static void drop() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE IF EXISTS multiPacketTest");
  }

  @Test
  public void bigByteSend() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE multiPacketTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        sharedConnBinary.prepareStatement("INSERT INTO multiPacketTest VALUE (?,?)")) {
      prep.setString(1, new String(arr2, 0, 128 * 1024 - 24));
      prep.setByte(2, (byte) 2);
      prep.execute();
    }
    ResultSet rs = stmt.executeQuery("SELECT t2 FROM multiPacketTest");
    rs.next();
    assertEquals("2", rs.getString(1));
  }

  @Test
  public void bigByte2Send() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE multiPacketTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        sharedConnBinary.prepareStatement("INSERT INTO multiPacketTest VALUE (?,?)")) {
      prep.setString(1, new String(arr2, 0, 16 * 1024 * 1024 - 21));
      prep.setByte(2, (byte) 2);
      prep.execute();
    }
    ResultSet rs = stmt.executeQuery("SELECT t2 FROM multiPacketTest");
    rs.next();
    assertEquals("2", rs.getString(1));
  }

  @Test
  public void bigShortSend() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE multiPacketTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        sharedConnBinary.prepareStatement("INSERT INTO multiPacketTest VALUE (?,?)")) {
      prep.setString(1, new String(arr2, 0, 16 * 1024 * 1024 - 21));
      prep.setShort(2, (short) 2);
      prep.execute();
    }
    ResultSet rs = stmt.executeQuery("SELECT t2 FROM multiPacketTest");
    rs.next();
    assertEquals("2", rs.getString(1));
  }

  @Test
  public void bigIntSend() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE multiPacketTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        sharedConnBinary.prepareStatement("INSERT INTO multiPacketTest VALUE (?,?)")) {
      prep.setString(1, new String(arr2, 0, 16 * 1024 * 1024 - 21));
      prep.setInt(2, 2);
      prep.execute();
    }
    ResultSet rs = stmt.executeQuery("SELECT t2 FROM multiPacketTest");
    rs.next();
    assertEquals("2", rs.getString(1));
  }

  @Test
  public void bigLongSend() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE multiPacketTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        sharedConnBinary.prepareStatement("INSERT INTO multiPacketTest VALUE (?,?)")) {
      prep.setString(1, new String(arr2, 0, 16 * 1024 * 1024 - 21));
      prep.setLong(2, 2L);
      prep.execute();
    }
    ResultSet rs = stmt.executeQuery("SELECT t2 FROM multiPacketTest");
    rs.next();
    assertEquals("2", rs.getString(1));
  }

  @Test
  public void bigStringSend() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE multiPacketTest");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        sharedConnBinary.prepareStatement("INSERT INTO multiPacketTest VALUE (?,?)")) {
      prep.setString(1, new String(arr2, 0, 16 * 1024 * 1024 - 21));
      prep.setString(2, new String(arr2, 0, 30_000));
      prep.execute();

      prep.setString(1, new String(arr2, 0, 16 * 1024 * 1024 - 21));
      prep.setString(2, new String(arr2, 0, 70_000));
      prep.execute();
      if (getMaxAllowedPacket() > 34 * 1024 * 1024) {
        prep.setString(1, new String(arr2, 0, 16 * 1024 * 1024 - 21));
        prep.setString(2, new String(arr2, 0, 17 * 1024 * 1024));
        prep.execute();
      }
    }
    ResultSet rs = stmt.executeQuery("SELECT t2 FROM multiPacketTest");
    rs.next();
    assertEquals(new String(arr2, 0, 30_000), rs.getString(1));
    rs.next();
    assertEquals(new String(arr2, 0, 70_000), rs.getString(1));
    rs.next();
    if (getMaxAllowedPacket() > 34 * 1024 * 1024) {
      assertEquals(new String(arr2, 0, 17 * 1024 * 1024), rs.getString(1));
    }
  }
}
