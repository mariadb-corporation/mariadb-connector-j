/*
 *
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

import static org.junit.Assert.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class BigQueryTest extends BaseTest {

  /**
   * Initialize test data.
   *
   * @throws SQLException id connection error occur
   */
  @BeforeClass()
  public static void initClass() throws SQLException {
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute(
          "CREATE TABLE bigblob(id int not null primary key auto_increment, test longblob)");
      stmt.execute(
          "CREATE TABLE bigblob2(id int not null primary key auto_increment, test longblob, test2 longblob)");
      stmt.execute(
          "CREATE TABLE bigblob3(id int not null primary key auto_increment, test longblob, test2 longblob, test3 varchar(20))");
      stmt.execute("CREATE TABLE bigblob4(test longblob)");
      stmt.execute(
          "CREATE TABLE bigblob5(id int not null primary key auto_increment, test longblob, test2 text)");
      stmt.execute(
          "CREATE TABLE bigblob6(id int not null primary key auto_increment, test longblob)");
      stmt.execute("FLUSH TABLE");
    }
  }

  @AfterClass
  public static void afterClass() throws SQLException {
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute("DROP TABLE bigblob");
      stmt.execute("DROP TABLE bigblob2");
      stmt.execute("DROP TABLE bigblob3");
      stmt.execute("DROP TABLE bigblob4");
      stmt.execute("DROP TABLE bigblob5");
      stmt.execute("DROP TABLE bigblob6");
    }
  }

  @Test
  public void sendBigQuery2() throws SQLException {

    Assume.assumeTrue(checkMaxAllowedPacketMore40m("sendBigQuery2"));

    char[] arr = new char[20000000];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = (char) ('a' + (i % 10));
    }

    Statement stmt = sharedConnection.createStatement();
    String query = "INSERT INTO bigblob VALUES (null, '" + String.valueOf(arr) + "')";

    stmt.executeUpdate(query);

    ResultSet rs = stmt.executeQuery("select * from bigblob");
    assertTrue(rs.next());
    byte[] newBytes = rs.getBytes(2);
    assertEquals(arr.length, newBytes.length);
    for (int i = 0; i < arr.length; i++) {
      assertEquals(arr[i], newBytes[i]);
    }
  }

  @Test
  public void sendBigPreparedQueryFe() throws SQLException {

    Assume.assumeTrue(checkMaxAllowedPacketMore20m("sendBigPreparedQueryFe"));

    byte[] arr = new byte[20000000];
    Arrays.fill(arr, (byte) 0xfe);
    try (Connection connection = setConnection("&useCompression=true")) {
      PreparedStatement ps = connection.prepareStatement("insert into bigblob6 values(null, ?)");
      ps.setBytes(1, arr);
      ps.executeUpdate();
      ps.setBytes(1, arr);
      ps.executeUpdate();

      Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery("select test from bigblob6");
      rs.next();
      byte[] newBytes = rs.getBytes(1);
      assertEquals(arr.length, newBytes.length);
      for (int i = 0; i < arr.length; i++) {
        assertEquals(arr[i], newBytes[i]);
      }
    }
  }

  @Test
  public void sendBigPreparedQuery() throws SQLException {

    Assume.assumeTrue(checkMaxAllowedPacketMore40m("sendBigPreparedQuery"));

    byte[] arr = new byte[20000000];
    Arrays.fill(arr, (byte) 'a');
    byte[] arr2 = new byte[20000000];
    Arrays.fill(arr2, (byte) 'b');

    PreparedStatement ps =
        sharedConnection.prepareStatement("insert into bigblob2 values(null, ?,?)");
    ps.setBytes(1, arr);
    ps.setBytes(2, arr2);
    ps.executeUpdate();
    Statement stmt = sharedConnection.createStatement();
    ResultSet rs = stmt.executeQuery("select * from bigblob2");
    assertTrue(rs.next());
    byte[] newBytes = rs.getBytes(2);
    byte[] newBytes2 = rs.getBytes(3);
    assertEquals(arr.length, newBytes.length);
    assertEquals(arr2.length, newBytes2.length);
    for (int i = 0; i < arr.length; i++) {
      assertEquals(arr[i], newBytes[i]);
    }
    for (int i = 0; i < arr2.length; i++) {
      assertEquals(arr2[i], newBytes2[i]);
    }
  }

  @Test
  public void sendBigBlobPreparedQuery() throws SQLException {
    Assume.assumeTrue(checkMaxAllowedPacketMore40m("sendBigPreparedQuery") && sharedUsePrepare());
    long maxAllowedPacket = 0;
    Statement st = sharedConnection.createStatement();
    ResultSet rs1 = st.executeQuery("select @@max_allowed_packet");
    if (rs1.next()) {
      maxAllowedPacket = rs1.getInt(1);
      Assume.assumeTrue(maxAllowedPacket < 512 * 1024 * 1024L);
    } else {
      fail();
    }

    byte[] arr = new byte[(int) maxAllowedPacket - 1000];
    int pos = 0;
    while (pos < maxAllowedPacket - 1000) {
      arr[pos] = (byte) ((pos % 132) + 40);
      pos++;
    }
    byte[] arr2 = new byte[(int) maxAllowedPacket - 1000];
    pos = 0;
    while (pos < maxAllowedPacket - 1000) {
      arr2[pos] = (byte) (((pos + 5) % 127) + 40);
      pos++;
    }

    PreparedStatement ps =
        sharedConnection.prepareStatement("insert into bigblob3 values(null, ?,?,?)");
    ps.setBlob(1, new MariaDbBlob(arr));
    ps.setBlob(2, new MariaDbBlob(arr2));
    ps.setString(3, "bob");
    ps.executeUpdate();
    Statement stmt = sharedConnection.createStatement();
    ResultSet rs = stmt.executeQuery("select * from bigblob3");
    assertTrue(rs.next());
    byte[] newBytes = rs.getBytes(2);
    byte[] newBytes2 = rs.getBytes(3);
    assertEquals(arr.length, newBytes.length);
    assertEquals(arr2.length, newBytes2.length);
    for (int i = 0; i < arr.length; i++) {
      assertEquals(arr[i], newBytes[i]);
    }
    for (int i = 0; i < arr2.length; i++) {
      assertEquals(arr2[i], newBytes2[i]);
    }
    assertEquals("bob", rs.getString(4));
  }

  @Test
  public void testError() throws SQLException {
    // check that maxAllowedPacket is big enough for the test
    Assume.assumeTrue(checkMaxAllowedPacketMore20m("testError"));

    try (Connection connection = setConnection()) {
      int selectSize = 9;
      char[] arr = new char[16 * 1024 * 1024 - selectSize];
      Arrays.fill(arr, 'a');
      String request = "select '" + new String(arr) + "'";
      ResultSet rs = connection.createStatement().executeQuery(request);
      assertTrue(rs.next());
      assertEquals(arr.length, rs.getString(1).length());
    }
  }

  @Test
  public void sendStreamComData() throws Exception {
    Assume.assumeTrue(checkMaxAllowedPacketMore40m("sendBigPreparedQuery") && sharedUsePrepare());

    File tmpFile = File.createTempFile("temp-file-name", ".tmp");
    byte[] bb = new byte[11000];
    for (int i = 0; i < 11_000; i++) {
      bb[i] = (byte) (i % 110 + 40);
    }

    try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
      for (int i = 0; i < 2_000; i++) {
        fos.write(bb);
      }
    }

    try (FileInputStream fis = new FileInputStream(tmpFile)) {
      try (FileInputStream fis2 = new FileInputStream(tmpFile)) {
        try (FileInputStream fis3 = new FileInputStream(tmpFile)) {
          try (PreparedStatement ps =
              sharedConnection.prepareStatement("insert into bigblob4 values(?)")) {

            // testing char stream
            ps.setCharacterStream(1, new InputStreamReader(fis, StandardCharsets.UTF_8));
            ps.executeUpdate();

            // testing byte stream
            ps.setBlob(1, fis2);
            ps.executeUpdate();

            // testing char stream with length
            ps.setCharacterStream(
                1, new InputStreamReader(fis3, StandardCharsets.UTF_8), 10_000_000);
            ps.executeUpdate();
          }
        }
      }
    }

    // test using binary resultSet
    PreparedStatement ps = sharedConnection.prepareStatement("select * from bigblob4");
    checkResult(tmpFile, ps.executeQuery(), 10_000_000);

    // test using text resultSet
    Statement stmt = sharedConnection.createStatement();
    checkResult(tmpFile, stmt.executeQuery("select * from bigblob4"), 10_000_000);
  }

  private void checkResult(File tmpFile, ResultSet rs, int length) throws Exception {
    assertTrue(rs.next());
    String res = rs.getString(1);
    try (Reader initialReader =
        new InputStreamReader(new FileInputStream(tmpFile), StandardCharsets.UTF_8)) {
      char[] bb = new char[64 * 1024];
      int len;
      int pos = 0;
      while ((len = initialReader.read(bb)) > 0) {
        for (int i = 0; i < len; i++) {
          assertEquals(bb[i], res.charAt(pos++));
        }
      }
    }

    assertTrue(rs.next());
    byte[] results = rs.getBytes(1);
    try (FileInputStream fis2 = new FileInputStream(tmpFile)) {
      byte[] byteBuffer = new byte[64 * 1024];
      int len;
      int pos = 0;
      while ((len = fis2.read(byteBuffer)) > 0) {
        for (int i = 0; i < len; i++) {
          assertEquals(byteBuffer[i], results[pos++]);
        }
      }
    }

    assertTrue(rs.next());
    res = rs.getString(1);
    assertEquals(length, res.length());
    try (Reader initialReader =
        new InputStreamReader(new FileInputStream(tmpFile), StandardCharsets.UTF_8)) {
      char[] bb = new char[64 * 1024];
      int len;
      int pos = 0;
      while ((len = initialReader.read(bb)) > 0) {
        for (int i = 0; i < len; i++) {
          if (pos < length) {
            assertEquals(bb[i], res.charAt(pos++));
          }
        }
      }
      assertEquals(length, pos);
    }

    assertFalse(rs.next());
  }

  @Test
  public void maxFieldSizeTest() throws SQLException {

    byte abyte = (byte) 'a';
    byte bbyte = (byte) 'b';

    byte[] arr = new byte[200];
    Arrays.fill(arr, abyte);
    byte[] arr2 = new byte[200];
    Arrays.fill(arr2, bbyte);

    PreparedStatement ps =
        sharedConnection.prepareStatement("insert into bigblob5 values(null, ?,?)");

    ps.setBytes(1, arr);
    ps.setBytes(2, arr2);
    ps.executeUpdate();

    Statement stmt = sharedConnection.createStatement();
    stmt.setMaxFieldSize(2);
    ResultSet rs = stmt.executeQuery("select * from bigblob5");
    assertTrue(rs.next());
    assertEquals(2, rs.getBytes(2).length);
    assertEquals(2, rs.getString(3).length());
    assertArrayEquals(new byte[] {abyte, abyte}, rs.getBytes(2));
    assertEquals("bb", rs.getString(3));
  }
}
