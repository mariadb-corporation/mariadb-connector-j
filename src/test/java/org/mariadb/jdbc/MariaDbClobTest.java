package org.mariadb.jdbc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;

public class MariaDbClobTest extends BaseTest {

  private final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);

  @Test
  public void length() {
    MariaDbClob clob = new MariaDbClob(bytes);
    assertEquals(10, clob.length());

    MariaDbClob clob2 = new MariaDbClob(bytes, 2, 3);
    assertEquals(3, clob2.length());
  }

  @Test
  public void getSubString() throws SQLException {
    MariaDbClob clob = new MariaDbClob(bytes);
    assertEquals("abcde🙏", clob.getSubString(1, 7));
    assertEquals("abcde🙏fgh", clob.getSubString(1, 20));
    assertEquals("abcde🙏fgh", clob.getSubString(1, (int) clob.length()));
    assertEquals("ab", clob.getSubString(1, 2));
    assertEquals("🙏", clob.getSubString(6, 2));

    MariaDbClob clob2 = new MariaDbClob(bytes, 4, 6);

    assertEquals("e🙏f", clob2.getSubString(1, 20));
    assertEquals("🙏f", clob2.getSubString(2, 3));

    try {
      clob2.getSubString(0, 3);
      fail("must have thrown exception, min pos is 1");
    } catch (SQLException sqle) {
      //normal exception
    }
  }

  @Test
  public void getCharacterStream() throws SQLException {
    MariaDbClob clob = new MariaDbClob(bytes);
    assureReaderEqual("abcde🙏", clob.getCharacterStream(1, 7));
    assureReaderEqual("abcde🙏fgh", clob.getCharacterStream(1, 10));
    try {
      assureReaderEqual("abcde🙏fgh", clob.getCharacterStream(1, 20));
      fail("must have throw exception, length > to number of characters");
    } catch (SQLException sqle) {
      //normal error
    }
    assureReaderEqual("bcde🙏", clob.getCharacterStream(2, 7));

    MariaDbClob clob2 = new MariaDbClob(bytes, 2, 9);
    assureReaderEqual("cde🙏fg", clob2.getCharacterStream(1, 7));
    try {
      assureReaderEqual("cde🙏fg", clob2.getCharacterStream(1, 20));
      fail("must have throw exception, length > to number of characters");
    } catch (SQLException sqle) {
      //normal error
    }

    assureReaderEqual("e🙏f", clob2.getCharacterStream(3, 5));

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
    final byte[] bytes =  "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);
    MariaDbClob clob = new MariaDbClob(bytes);
    assureReaderEqual("abcde🙏", clob.getCharacterStream(1, 7));

    Writer writer = clob.setCharacterStream(2);
    writer.write("tuvxyz", 2, 3);
    writer.flush();
    assertEquals("avxye🙏", clob.getSubString(1, 7));

    clob = new MariaDbClob(bytes);

    writer = clob.setCharacterStream(2);
    writer.write("1234567890lmnopqrstu", 1, 19);
    writer.flush();
    assertEquals("a234567890lmnopqrstu", clob.getSubString(1, 100));

  }


  @Test
  public void position() throws SQLException {
    MariaDbClob clob = new MariaDbClob(bytes);
    assertEquals(4, clob.position("de", 2));

    clob = new MariaDbClob(bytes, 2, 10);
    assertEquals(4, clob.position("🙏", 2));
  }

  @Test
  public void setString() throws SQLException {
    final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);
    MariaDbClob clob = new MariaDbClob(bytes);
    assureReaderEqual("abcde🙏", clob.getCharacterStream(1, 7));
    clob.setString(2, "zuv");
    assertEquals("azuve🙏", clob.getSubString(1, 7));
    clob.setString(9, "zzz");
    assertEquals("azuve🙏fgzzz", clob.getSubString(1, 12));

    clob = new MariaDbClob("abcde🙏fgh".getBytes(StandardCharsets.UTF_8), 2, 9);
    assureReaderEqual("cde🙏fg", clob.getCharacterStream(1, 7));
    assertEquals("cde🙏fg", clob.getSubString(1, 7));

    clob.setString(2, "zg");
    assertEquals("czg🙏f", clob.getSubString(1, 6));
    clob.setString(7, "zzz");
    String ss = clob.getSubString(1, 12);
    assertEquals("czg🙏fgzzz", clob.getSubString(1, 12));
  }

  @Test
  public void setAsciiStream() throws SQLException, IOException {
    final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);
    MariaDbClob clob = new MariaDbClob(bytes);
    assureReaderEqual("abcde🙏", clob.getCharacterStream(1, 7));

    OutputStream stream = clob.setAsciiStream(2);
    stream.write("tuvxyz".getBytes(), 2, 3);
    stream.flush();

    assertEquals("avxye🙏", clob.getSubString(1, 7));

    clob = new MariaDbClob(bytes);

    stream = clob.setAsciiStream(2);
    stream.write("1234567890lmnopqrstu".getBytes(), 1, 19);
    stream.flush();
    assertEquals("a234567890lmnopqrstu", clob.getSubString(1, 100));

  }

  @Test
  public void setBinaryStream() throws SQLException, IOException {
    final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);
    final byte[] otherBytes = new byte[]{10, 11, 12, 13};

    MariaDbClob blob = new MariaDbClob(new byte[]{0, 1, 2, 3, 4, 5});
    OutputStream out = blob.setBinaryStream(2);
    out.write(otherBytes);
    assertArrayEquals(new byte[]{0, 10, 11, 12, 13, 5}, blob.getBytes(1, 6));

    MariaDbClob blob2 = new MariaDbClob(new byte[]{0, 1, 2, 3, 4, 5});
    OutputStream out2 = blob2.setBinaryStream(4);
    out2.write(otherBytes);
    assertArrayEquals(new byte[]{0, 1, 2, 10, 11, 12, 13}, blob2.getBytes(1, 7));

    MariaDbClob blob3 = new MariaDbClob(new byte[]{0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out3 = blob3.setBinaryStream(2);
    out3.write(otherBytes);
    assertArrayEquals(new byte[]{2, 10, 11, 12, 13, 0, 0}, blob3.getBytes(1, 7));

    MariaDbClob blob4 = new MariaDbClob(new byte[]{0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out4 = blob4.setBinaryStream(4);
    out4.write(otherBytes);
    assertArrayEquals(new byte[]{2, 3, 4, 10, 11, 12}, blob4.getBytes(1, 6));

    try {
      MariaDbClob blob5 = new MariaDbClob(new byte[]{0, 1, 2, 3, 4, 5}, 2, 3);
      blob5.setBinaryStream(0);
    } catch (SQLException sqle) {
      //normal exception
    }
  }


  @Test
  public void setBinaryStreamOffset() throws SQLException, IOException {
    final byte[] bytes = "abcde🙏fgh".getBytes(StandardCharsets.UTF_8);
    final byte[] otherBytes = new byte[]{10, 11, 12, 13};

    MariaDbClob blob = new MariaDbClob(new byte[]{0, 1, 2, 3, 4, 5});
    OutputStream out = blob.setBinaryStream(2);
    out.write(otherBytes, 2, 3);
    assertArrayEquals(new byte[]{0, 12, 13, 3, 4, 5}, blob.getBytes(1, 6));

    MariaDbClob blob2 = new MariaDbClob(new byte[]{0, 1, 2, 3, 4, 5});
    OutputStream out2 = blob2.setBinaryStream(4);
    out2.write(otherBytes, 3, 2);
    assertArrayEquals(new byte[]{0, 1, 2, 13, 4, 5, 0}, blob2.getBytes(1, 7));

    MariaDbClob blob3 = new MariaDbClob(new byte[]{0, 1, 2, 3, 4, 5}, 2, 4);
    OutputStream out3 = blob3.setBinaryStream(2);
    out3.write(otherBytes, 2, 3);
    assertArrayEquals(new byte[]{2, 12, 13, 5, 0, 0, 0}, blob3.getBytes(1, 7));

    MariaDbClob blob4 = new MariaDbClob(new byte[]{0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out4 = blob4.setBinaryStream(4);
    out4.write(otherBytes, 2, 2);
    assertArrayEquals(new byte[]{2, 3, 4, 12, 13, 0}, blob4.getBytes(1, 6));

    MariaDbClob blob5 = new MariaDbClob(new byte[]{0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out5 = blob5.setBinaryStream(4);
    out5.write(otherBytes, 2, 20);
    assertArrayEquals(new byte[]{2, 3, 4, 12, 13, 0}, blob5.getBytes(1, 6));

  }

  @Test
  public void truncate() throws SQLException {
    MariaDbClob clob = new MariaDbClob(bytes);
    clob.truncate(20);
    assertEquals("abcde🙏f", clob.getSubString(1, 8));
    clob.truncate(8);
    assertEquals("abcde🙏f", clob.getSubString(1, 8));
    assertEquals("abcde🙏", clob.getSubString(1, 7));
    clob.truncate(7);
    assertEquals("abcde🙏", clob.getSubString(1, 8));
    clob.truncate(6);
    assertEquals("abcde�", clob.getSubString(1, 8));
    clob.truncate(4);
    assertEquals("abcd", clob.getSubString(1, 7));
    clob.truncate(0);
    assertEquals("", clob.getSubString(1, 7));

    MariaDbClob clob2 = new MariaDbClob("abcde🙏fgh".getBytes(StandardCharsets.UTF_8), 2,
        8);
    clob2.truncate(20);
    assertEquals("cde🙏f", clob2.getSubString(1, 8));
    clob2.truncate(6);
    assertEquals("cde🙏f", clob2.getSubString(1, 8));
    clob2.truncate(5);
    assertEquals("cde🙏", clob2.getSubString(1, 8));
    clob2.truncate(4);
    assertEquals("cde�", clob2.getSubString(1, 8));
    clob2.truncate(0);
    assertEquals("", clob2.getSubString(1, 7));

  }

  @Test
  public void free() {
    MariaDbClob blob = new MariaDbClob(bytes);
    blob.free();
    assertEquals(0, blob.length);
  }

  @Test
  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  public void clobLength() throws Exception {
    Statement stmt = sharedConnection.createStatement();
    try (ResultSet rs = stmt
        .executeQuery("SELECT 'ab$c', 'ab¢c', 'abहc', 'ab\uD801\uDC37c', 'ab𐍈c' from dual")) {
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

}
