// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.SingleStoreClob;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

public class ClobTest extends Common {

  private final byte[] bytes = "abc£de🙏fgh".getBytes(StandardCharsets.UTF_8);

  @Test
  public void length() throws SQLException {
    SingleStoreClob clob = new SingleStoreClob(bytes);
    assertEquals(11, clob.length());

    SingleStoreClob clob2 = new SingleStoreClob(bytes, 2, 3);
    assertEquals(2, clob2.length());
  }

  @Test
  public void getSubString() throws SQLException {
    SingleStoreClob clob = new SingleStoreClob(bytes);
    assertEquals("abc£de🙏", clob.getSubString(1, 8));
    assertEquals("abc£de🙏fgh", clob.getSubString(1, 21));
    assertEquals("abc£de🙏fgh", clob.getSubString(1, (int) clob.length()));
    assertEquals("ab", clob.getSubString(1, 2));
    assertEquals("🙏", clob.getSubString(7, 2));

    SingleStoreClob clob2 = new SingleStoreClob(bytes, 6, 6);

    assertEquals("e🙏f", clob2.getSubString(1, 20));
    assertEquals("🙏f", clob2.getSubString(2, 3));

    try {
      clob2.getSubString(0, 3);
      fail("must have thrown exception, min pos is 1");
    } catch (SQLException sqle) {
      // normal exception
    }
  }

  @Test
  public void getCharacterStream() throws SQLException {
    SingleStoreClob clob = new SingleStoreClob(bytes);
    assureReaderEqual("abc£de🙏", clob.getCharacterStream(1, 8));
    assureReaderEqual("abc£de🙏fgh", clob.getCharacterStream(1, 11));
    try {
      assureReaderEqual("abc£de🙏fgh", clob.getCharacterStream(1, 20));
      fail("must have throw exception, length > to number of characters");
    } catch (SQLException sqle) {
      // normal error
    }
    assureReaderEqual("bc£de🙏", clob.getCharacterStream(2, 7));

    SingleStoreClob clob2 = new SingleStoreClob(bytes, 2, 9);
    assureReaderEqual("c£de🙏", clob2.getCharacterStream(1, 6));
    try {
      assureReaderEqual("c£de🙏fg", clob2.getCharacterStream(1, 20));
      fail("must have throw exception, length > to number of characters");
    } catch (SQLException sqle) {
      // normal error
    }

    assureReaderEqual("de🙏", clob2.getCharacterStream(3, 4));
  }

  private void assureReaderEqual(String expectedStr, Reader reader) {
    try {
      char[] expected = expectedStr.toCharArray();
      char[] readArr = new char[expected.length];
      assertEquals(expected.length, reader.read(readArr));
      assertArrayEquals(expected, readArr);
    } catch (IOException ioe) {
      ioe.printStackTrace();
      fail();
    }
  }

  @Test
  public void setCharacterStream() throws SQLException, IOException {
    final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);
    SingleStoreClob clob = new SingleStoreClob(bytes);
    assureReaderEqual("abcde🙏", clob.getCharacterStream(1, 7));

    Writer writer = clob.setCharacterStream(2);
    writer.write("tuvxyz", 2, 3);
    writer.flush();
    assertEquals("avxye🙏", clob.getSubString(1, 7));

    clob = new SingleStoreClob(bytes);

    writer = clob.setCharacterStream(2);
    writer.write("1234567890lmnopqrstu", 1, 19);
    writer.flush();
    assertEquals("a234567890lmnopqrstu", clob.getSubString(1, 100));
  }

  @Test
  public void position() throws SQLException {
    SingleStoreClob clob = new SingleStoreClob(bytes);
    assertEquals(5, clob.position("de", 2));
    assertEquals(5, clob.position((Clob) new SingleStoreClob("de".getBytes()), 2));

    clob = new SingleStoreClob(bytes, 2, 10);
    assertEquals(5, clob.position("🙏", 2));
  }

  @Test
  public void setString() throws SQLException {
    final byte[] bytes = "abcd£e🙏fgh".getBytes(StandardCharsets.UTF_8);
    final SingleStoreClob clob = new SingleStoreClob(bytes);
    assureReaderEqual("abcd£e🙏", clob.getCharacterStream(1, 8));
    clob.setString(2, "zuv");
    assertEquals("azuv£e🙏", clob.getSubString(1, 8));
    clob.setString(9, "zzz");
    assertEquals("azuv£e🙏fzzz", clob.getSubString(1, 12));

    SingleStoreClob clob2 =
        new SingleStoreClob("abcde🙏fgh".getBytes(StandardCharsets.UTF_8), 2, 9);
    assureReaderEqual("cde🙏fg", clob2.getCharacterStream(1, 7));
    assertEquals("cde🙏fg", clob2.getSubString(1, 7));

    clob2.setString(2, "zg");
    assertEquals("czg🙏f", clob2.getSubString(1, 6));
    clob2.setString(7, "zzz");
    String ss = clob2.getSubString(1, 12);
    assertEquals("czg🙏fgzzz", clob2.getSubString(1, 12));

    Common.assertThrowsContains(
        SQLException.class, () -> clob2.setString(2, "abcd", 2, -2), "len must be > 0");
    clob2.setString(2, "abcd", 2, 2);
    assertEquals("ccd🙏f", clob2.getSubString(1, 6));
    clob2.setString(2, "opml", 3, 200);
    assertEquals("cld🙏f", clob2.getSubString(1, 6));

    clob2.setString(5, "අข\uD800\uDFA2");
    assertEquals("cld🙏අข\uD800\uDFA2", clob2.getSubString(1, 20));
    assertEquals(9, clob2.length());
    clob2.setString(6, "ข\uD800\uDFA2");
    assertEquals("cld🙏අข\uD800\uDFA2", clob2.getSubString(1, 20));
    clob2.setString(7, "\uD800\uDFA2");
    assertEquals("cld🙏අข\uD800\uDFA2", clob2.getSubString(1, 20));
    clob2.truncate(9);
    assertEquals("cld🙏අข\uD800\uDFA2", clob2.getSubString(1, 20));
    clob2.truncate(7);
    assertEquals("cld🙏අข", clob2.getSubString(1, 20));
    clob2.truncate(6);
    assertEquals("cld🙏අ", clob2.getSubString(1, 20));
    clob2.truncate(5);
    assertEquals("cld🙏", clob2.getSubString(1, 20));
    clob2.truncate(3);
    assertEquals("cld", clob2.getSubString(1, 20));

    Common.assertThrowsContains(
        SQLException.class, () -> clob.setString(-1, "7"), "position must be >= 0");
    Common.assertThrowsContains(
        SQLException.class, () -> clob.setString(1, null), "cannot add null string");
    Common.assertThrowsContains(
        SQLException.class, () -> clob.setString(-1, null, 1, 2), "cannot add null string");
    Common.assertThrowsContains(
        SQLException.class, () -> clob.setString(0, "dd", -1, 2), "offset must be >= 0");
    Common.assertThrowsContains(
        SQLException.class, () -> clob.getSubString(-1, 7), "position must be >= 1");
    Common.assertThrowsContains(
        SQLException.class, () -> clob.getSubString(1, -7), "length must be > 0");
    Common.assertThrowsContains(
        SQLException.class, () -> clob.setString(-2, "rrr"), "position must be >= 0");
  }

  @Test
  public void setAsciiStream() throws SQLException, IOException {
    final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);
    SingleStoreClob clob = new SingleStoreClob(bytes);
    assureReaderEqual("abcde🙏", clob.getCharacterStream(1, 7));

    OutputStream stream = clob.setAsciiStream(2);
    stream.write("tuvxyz".getBytes(), 2, 3);
    stream.flush();

    assertEquals("avxye🙏", clob.getSubString(1, 7));

    clob = new SingleStoreClob(bytes);

    stream = clob.setAsciiStream(2);
    stream.write("1234567890lmnopqrstu".getBytes(), 1, 19);
    stream.flush();
    assertEquals("a234567890lmnopqrstu", clob.getSubString(1, 100));

    SingleStoreClob clob2 = new SingleStoreClob(bytes);
    InputStream stream2 = clob2.getAsciiStream();
    byte[] b = new byte[12];
    stream2.read(b);
    assertArrayEquals(bytes, b);
  }

  @Test
  public void wrongUtf8() {
    final byte[] utf8Wrong2bytes = new byte[] {0x08, (byte) 0xFF, (byte) 0x6F, (byte) 0x6F};
    final byte[] utf8Wrong3bytes =
        new byte[] {0x07, (byte) 0x0a, (byte) 0xff, (byte) 0x6F, (byte) 0x6F};
    final byte[] utf8Wrong4bytes =
        new byte[] {0x10, (byte) 0x20, (byte) 0x0a, (byte) 0xff, (byte) 0x6F, (byte) 0x6F};
    final byte[] utf8Wrong4bytes2 = new byte[] {-16, (byte) -97, (byte) -103};

    Common.assertThrowsContains(
        UncheckedIOException.class,
        () -> new SingleStoreClob(utf8Wrong2bytes).length(),
        "invalid UTF8");
    Common.assertThrowsContains(
        UncheckedIOException.class,
        () -> new SingleStoreClob(new byte[] {(byte) 225}).length(),
        "invalid UTF8");

    Common.assertThrowsContains(
        UncheckedIOException.class,
        () -> new SingleStoreClob(utf8Wrong3bytes).length(),
        "invalid UTF8");
    Common.assertThrowsContains(
        UncheckedIOException.class,
        () -> new SingleStoreClob(utf8Wrong4bytes).length(),
        "invalid UTF8");
    Common.assertThrowsContains(
        UncheckedIOException.class,
        () -> new SingleStoreClob(new byte[] {(byte) 225}).truncate(2),
        "invalid UTF8");
    Common.assertThrowsContains(
        UncheckedIOException.class,
        () -> new SingleStoreClob(utf8Wrong2bytes).truncate(2),
        "invalid UTF8");
    Common.assertThrowsContains(
        UncheckedIOException.class,
        () -> new SingleStoreClob(utf8Wrong3bytes).truncate(3),
        "invalid UTF8");
    Common.assertThrowsContains(
        UncheckedIOException.class,
        () -> new SingleStoreClob(utf8Wrong4bytes2).truncate(4),
        "invalid UTF8");
  }

  @Test
  public void setBinaryStream() throws SQLException, IOException {
    final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);
    final byte[] otherBytes = new byte[] {10, 11, 12, 13};

    SingleStoreClob blob = new SingleStoreClob(new byte[] {0, 1, 2, 3, 4, 5});
    OutputStream out = blob.setBinaryStream(2);
    out.write(otherBytes);
    assertArrayEquals(new byte[] {0, 10, 11, 12, 13, 5}, blob.getBytes(1, 6));

    SingleStoreClob blob2 = new SingleStoreClob(new byte[] {0, 1, 2, 3, 4, 5});
    OutputStream out2 = blob2.setBinaryStream(4);
    out2.write(otherBytes);
    assertArrayEquals(new byte[] {0, 1, 2, 10, 11, 12, 13}, blob2.getBytes(1, 7));

    SingleStoreClob blob3 = new SingleStoreClob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out3 = blob3.setBinaryStream(2);
    out3.write(otherBytes);
    assertArrayEquals(new byte[] {2, 10, 11, 12, 13, 0, 0}, blob3.getBytes(1, 7));

    SingleStoreClob blob4 = new SingleStoreClob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out4 = blob4.setBinaryStream(4);
    out4.write(otherBytes);
    assertArrayEquals(new byte[] {2, 3, 4, 10, 11, 12}, blob4.getBytes(1, 6));

    try {
      SingleStoreClob blob5 = new SingleStoreClob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
      blob5.setBinaryStream(0);
    } catch (SQLException sqle) {
      // normal exception
    }
  }

  @Test
  public void setBinaryStreamOffset() throws SQLException, IOException {
    final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);
    final byte[] otherBytes = new byte[] {10, 11, 12, 13};

    SingleStoreClob blob = new SingleStoreClob(new byte[] {0, 1, 2, 3, 4, 5});
    OutputStream out = blob.setBinaryStream(2);
    out.write(otherBytes, 2, 3);
    assertArrayEquals(new byte[] {0, 12, 13, 3, 4, 5}, blob.getBytes(1, 6));

    SingleStoreClob blob2 = new SingleStoreClob(new byte[] {0, 1, 2, 3, 4, 5});
    OutputStream out2 = blob2.setBinaryStream(4);
    out2.write(otherBytes, 3, 2);
    assertArrayEquals(new byte[] {0, 1, 2, 13, 4, 5, 0}, blob2.getBytes(1, 7));

    SingleStoreClob blob3 = new SingleStoreClob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 4);
    OutputStream out3 = blob3.setBinaryStream(2);
    out3.write(otherBytes, 2, 3);
    assertArrayEquals(new byte[] {2, 12, 13, 5, 0, 0, 0}, blob3.getBytes(1, 7));

    SingleStoreClob blob4 = new SingleStoreClob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out4 = blob4.setBinaryStream(4);
    out4.write(otherBytes, 2, 2);
    assertArrayEquals(new byte[] {2, 3, 4, 12, 13, 0}, blob4.getBytes(1, 6));

    SingleStoreClob blob5 = new SingleStoreClob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out5 = blob5.setBinaryStream(4);
    out5.write(otherBytes, 2, 20);
    assertArrayEquals(new byte[] {2, 3, 4, 12, 13, 0}, blob5.getBytes(1, 6));
  }

  @Test
  public void truncate() throws SQLException {
    SingleStoreClob clob = new SingleStoreClob(bytes);
    clob.truncate(20);
    assertEquals("abc£de🙏f", clob.getSubString(1, 9));
    clob.truncate(8);
    assertEquals("abc£de🙏", clob.getSubString(1, 9));
    assertEquals("abc£de🙏", clob.getSubString(1, 8));
    clob.truncate(7);
    assertEquals("abc£de�", clob.getSubString(1, 9));
    clob.truncate(6);
    assertEquals("abc£de", clob.getSubString(1, 9));
    clob.truncate(4);
    assertEquals("abc£", clob.getSubString(1, 8));
    clob.truncate(3);
    assertEquals("abc", clob.getSubString(1, 8));
    clob.truncate(0);
    assertEquals("", clob.getSubString(1, 8));

    SingleStoreClob clob2 =
        new SingleStoreClob("abc£de🙏fgh".getBytes(StandardCharsets.UTF_8), 2, 10);
    clob2.truncate(20);
    assertEquals("c£de🙏f", clob2.getSubString(1, 9));
    clob2.truncate(6);
    assertEquals("c£de🙏", clob2.getSubString(1, 9));
    clob2.truncate(5);
    assertEquals("c£de�", clob2.getSubString(1, 9));
    clob2.truncate(4);
    assertEquals("c£de", clob2.getSubString(1, 9));
    clob2.truncate(0);
    assertEquals("", clob2.getSubString(1, 7));
  }

  @Test
  public void free() throws SQLException {
    SingleStoreClob blob = new SingleStoreClob(bytes);
    blob.free();
    assertEquals(0, blob.length());
  }

  @Test
  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  public void clobLength() throws Exception {
    Statement stmt = sharedConn.createStatement();
    try (ResultSet rs =
        stmt.executeQuery("SELECT 'ab$c', 'ab¢c', 'abहc', 'ab\uD801\uDC37c', 'ab𐍈c' from dual")) {
      while (rs.next()) {

        Clob clob1 = rs.getClob(1);
        assertEquals(4, clob1.length());

        Clob clob2 = rs.getClob(2);
        assertEquals(4, clob2.length());

        Clob clob3 = rs.getClob(3);
        assertEquals(4, clob3.length());

        Clob clob4 = rs.getClob(4);
        assertEquals(5, clob4.length());

        Clob clob5 = rs.getClob(5);
        assertEquals(5, clob5.length());

        clob1.truncate(3);
        clob2.truncate(3);
        clob3.truncate(3);
        clob4.truncate(3);
        clob5.truncate(3);

        assertEquals(3, clob1.length());
        assertEquals(3, clob2.length());
        assertEquals(3, clob3.length());
        assertEquals(3, clob4.length());
        assertEquals(3, clob5.length());

        assertEquals("ab$", clob1.getSubString(1, 3));
        assertEquals("ab¢", clob2.getSubString(1, 3));
        assertEquals("abह", clob3.getSubString(1, 3));
        assertEquals("ab�", clob4.getSubString(1, 3));
        assertEquals("ab�", clob5.getSubString(1, 3));
      }
    }
  }

  @Test
  public void equal() {
    SingleStoreClob clob = new SingleStoreClob(bytes);
    assertEquals(clob, clob);
    assertEquals(new SingleStoreClob(bytes), clob);
    assertFalse(clob.equals(null));
    assertFalse(clob.equals(""));
    byte[] bytes = "Abc£de🙏fgh".getBytes(StandardCharsets.UTF_8);
    assertNotEquals(new SingleStoreClob(bytes), clob);
    assertNotEquals(new SingleStoreClob("Abc".getBytes(StandardCharsets.UTF_8)), clob);
  }
}
