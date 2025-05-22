// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.MariaDbResultSet;
import org.mariadb.jdbc.Statement;

public class BlobTest extends Common {

  private final byte[] bytes = new byte[] {0, 1, 2, 3, 4, 5};

  @Test
  public void length() {
    MariaDbBlob blob = new MariaDbBlob(bytes);
    assertEquals(6, blob.length());

    MariaDbBlob blob2 = new MariaDbBlob(bytes, 2, 3);
    assertEquals(3, blob2.length());
  }

  @Test
  public void getBytes() throws SQLException {
    MariaDbBlob blob = new MariaDbBlob(bytes);
    assertArrayEquals(bytes, blob.getBytes(1, 6));
    assertArrayEquals(new byte[] {0, 1, 2, 3, 4, 5, 0}, blob.getBytes(1, 7));
    assertArrayEquals(new byte[] {0, 1}, blob.getBytes(1, 2));

    MariaDbBlob blob2 = new MariaDbBlob(bytes, 2, 3);
    assertArrayEquals(new byte[] {2, 3, 4}, blob2.getBytes(1, 3));
    assertArrayEquals(new byte[] {2, 3, 4, 0, 0, 0}, blob2.getBytes(1, 6));
    assertArrayEquals(new byte[] {2, 3}, blob2.getBytes(1, 2));
    assertArrayEquals(new byte[] {3, 4, 0}, blob2.getBytes(2, 3));
    assertArrayEquals(new byte[] {3, 4, 0, 0, 0, 0}, blob2.getBytes(2, 6));
    assertArrayEquals(new byte[] {3, 4}, blob2.getBytes(2, 2));

    try {
      blob2.getBytes(0, 3);
      fail("must have thrown exception, min pos is 1");
    } catch (SQLException sqle) {
      // normal exception
    }
  }

  @Test
  public void getBinaryStream() throws SQLException {
    MariaDbBlob blob = new MariaDbBlob(bytes);
    assureInputStreamEqual(bytes, blob.getBinaryStream(1, 6));
    Common.assertThrowsContains(
        SQLException.class,
        () -> assureInputStreamEqual(new byte[] {0, 1, 2, 3, 4, 5, 0}, blob.getBinaryStream(1, 7)),
        "Out of range (position + length - 1 > streamSize)");
    Common.assertThrowsContains(
        SQLException.class,
        () -> assureInputStreamEqual(new byte[] {0, 1, 2, 3, 4, 5, 0}, blob.getBinaryStream(-2, 7)),
        "Out of range (position should be > 0)");
    Common.assertThrowsContains(
        SQLException.class,
        () -> assureInputStreamEqual(new byte[] {0, 1, 2, 3, 4, 5, 0}, blob.getBinaryStream(20, 7)),
        "Out of range (position > stream size)");

    assureInputStreamEqual(new byte[] {0, 1}, blob.getBinaryStream(1, 2));

    MariaDbBlob blob2 = new MariaDbBlob(bytes, 2, 3);
    assureInputStreamEqual(new byte[] {2, 3, 4}, blob2.getBinaryStream(1, 3));
    try {
      assureInputStreamEqual(new byte[] {2, 3, 4, 0, 0, 0}, blob2.getBinaryStream(1, 6));
      fail("must have thrown exception, max length is 3");
    } catch (SQLException sqle) {
      // normal exception
    }
    assureInputStreamEqual(new byte[] {2, 3}, blob2.getBinaryStream(1, 2));
    try {
      assureInputStreamEqual(new byte[] {3, 4, 0}, blob2.getBinaryStream(2, 3));
    } catch (SQLException sqle) {
      // normal exception
    }
    assureInputStreamEqual(new byte[] {3, 4}, blob2.getBinaryStream(2, 2));

    try {
      blob2.getBytes(0, 3);
      fail("must have thrown exception, min pos is 1");
    } catch (SQLException sqle) {
      // normal exception
    }
  }

  @Test
  public void blobStream() throws SQLException, IOException {
    int maxAllowedPacket = getMaxAllowedPacket();
    Assumptions.assumeTrue(maxAllowedPacket > 40 * 1024 * 1024);
    blobStream(sharedConn);
    blobStream(sharedConnBinary);
  }

  public void blobStream(Connection con) throws SQLException, IOException {
    Statement stmt = con.createStatement(MariaDbResultSet.TYPE_SEQUENTIAL_ACCESS_ONLY, ResultSet.CONCUR_READ_ONLY);
    int LENGTH = 50 * 1024 * 1024;
    stmt.execute("DROP TABLE IF EXISTS blobStream");
    stmt.execute("CREATE TABLE blobStream (t1 int not null primary key auto_increment, t2 LONGTEXT)");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO blobStream VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setBinaryStream(2, new RepeatingCharacterInputStream('a', LENGTH));
      prep.execute();
    }

    try (ResultSet rs = stmt.executeQuery("SELECT t2, t1 from blobStream WHERE t1 = 1")) {
      assertTrue(rs.next());
      Blob blob = rs.getBlob(1);
      assertEquals(LENGTH, blob.length());
      InputStream is = blob.getBinaryStream();
      int read;
      int total = 0;
      byte[] b = new byte[8192];
      do {
        read = is.read(b);
        if (read > 0) {
          total += read;
        }
      } while (read != -1);
      assertEquals(LENGTH, total);
      assertEquals(-1, is.read(b));
      assertEquals(1, rs.getInt(2));
    }

    try (ResultSet rs = stmt.executeQuery("SELECT t2, t1 from blobStream WHERE t1 = 1")) {
      assertTrue(rs.next());
      Blob blob = rs.getBlob(1);
      assertEquals(LENGTH, blob.length());
      InputStream is = blob.getBinaryStream();
      int read;
      int total = 0;
      byte[] b = new byte[8192];

      read = is.read(b);
      total += read;

      // force loading into memory
      assertEquals(1, rs.getInt(2));

      do {
        read = is.read(b);
        if (read > 0) {
          total += read;
        }
      } while (read != -1);
      assertEquals(LENGTH, total);

    }

    con.commit();
  }

  private class RepeatingCharacterInputStream extends InputStream {
    private final char character;
    private final long size;
    private long position = 0;

    public RepeatingCharacterInputStream(char character, long size) {
      this.character = character;
      this.size = size;
    }

    @Override
    public int read() throws IOException {
      if (position < size) {
        position++;
        return character;
      } else {
        return -1; // End of stream
      }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (position >= size) {
        return -1; // End of stream
      }

      int bytesToRead = (int) Math.min(len, size - position);
      byte value = (byte) character;

      for (int i = 0; i < bytesToRead; i++) {
        b[off + i] = value;
      }

      position += bytesToRead;
      return bytesToRead;
    }
  }

  private void assureInputStreamEqual(byte[] expected, InputStream stream) {
    try {
      for (byte expectedVal : expected) {
        int val = stream.read();
        assertEquals(expectedVal, val);
      }
      assertEquals(-1, stream.read());
    } catch (IOException ioe) {
      ioe.printStackTrace();
      fail();
    }
  }

  @Test
  public void position() throws SQLException {
    MariaDbBlob blob = new MariaDbBlob(bytes);
    assertEquals(5, blob.position(new byte[] {4, 5}, 2));

    MariaDbBlob blob2 = new MariaDbBlob(bytes, 2, 4);
    assertEquals(3, blob2.position(new byte[] {4, 5}, 2));
    assertEquals(0, blob2.position(new byte[0], 2));
    assertEquals(-1, blob2.position(new byte[] {4, 9}, 2));

    assertEquals(3, blob2.position(new MariaDbBlob(new byte[] {4, 5}), 2));

    Common.assertThrowsContains(
        SQLException.class,
        () -> blob2.position(new byte[] {4, 5}, -2),
        "Out of range (position should be > 0, " + "but is -2)");
    Common.assertThrowsContains(
        SQLException.class,
        () -> blob2.position(new byte[] {4, 5}, 20),
        "Out of range (start > stream size)");
  }

  @Test
  public void setBytes() throws SQLException {
    final byte[] otherBytes = new byte[] {10, 11, 12, 13};

    MariaDbBlob blob = new MariaDbBlob(new byte[0]);
    blob.setBytes(1, new byte[0]);
    assertEquals(0, blob.length());
    blob.setBytes(1, new byte[0], 0, 0);
    assertEquals(0, blob.length());

    blob = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5});
    blob.setBytes(2, otherBytes);
    assertArrayEquals(new byte[] {0, 10, 11, 12, 13, 5}, blob.getBytes(1, 6));

    MariaDbBlob blob2 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5});
    blob2.setBytes(4, otherBytes);
    assertArrayEquals(new byte[] {0, 1, 2, 10, 11, 12, 13}, blob2.getBytes(1, 7));

    MariaDbBlob blob3 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    blob3.setBytes(2, otherBytes);
    assertArrayEquals(new byte[] {2, 10, 11, 12, 13, 0, 0}, blob3.getBytes(1, 7));

    MariaDbBlob blob4 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    blob4.setBytes(4, otherBytes);
    assertArrayEquals(new byte[] {2, 3, 4, 10, 11, 12}, blob4.getBytes(1, 6));

    try {
      MariaDbBlob blob5 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
      blob5.setBytes(0, otherBytes);
    } catch (SQLException sqle) {
      // normal exception
    }
  }

  @Test
  public void setBytesOffset() throws SQLException {
    final byte[] otherBytes = new byte[] {10, 11, 12, 13};

    MariaDbBlob blob = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5});
    blob.setBytes(2, otherBytes, 2, 3);
    assertArrayEquals(new byte[] {0, 12, 13, 3, 4, 5}, blob.getBytes(1, 6));

    MariaDbBlob blob2 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5});
    blob2.setBytes(4, otherBytes, 3, 2);
    assertArrayEquals(new byte[] {0, 1, 2, 13, 4, 5, 0}, blob2.getBytes(1, 7));

    MariaDbBlob blob3 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 4);
    blob3.setBytes(2, otherBytes, 2, 3);
    assertArrayEquals(new byte[] {2, 12, 13, 5, 0, 0, 0}, blob3.getBytes(1, 7));

    MariaDbBlob blob4 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    blob4.setBytes(4, otherBytes, 2, 2);
    assertArrayEquals(new byte[] {2, 3, 4, 12, 13, 0}, blob4.getBytes(1, 6));

    MariaDbBlob blob5 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    blob5.setBytes(4, otherBytes, 2, 20);
    assertArrayEquals(new byte[] {2, 3, 4, 12, 13, 0}, blob5.getBytes(1, 6));

    try {
      MariaDbBlob blob6 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
      blob6.setBytes(0, otherBytes, 2, 3);
    } catch (SQLException sqle) {
      // normal exception
    }
  }

  @Test
  public void setBinaryStream() throws SQLException, IOException {
    final byte[] otherBytes = new byte[] {10, 11, 12, 13};

    MariaDbBlob blob = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5});
    OutputStream out = blob.setBinaryStream(2);
    out.write(otherBytes);
    out.write(0x09);
    out.write(0x08);
    assertArrayEquals(new byte[] {0, 10, 11, 12, 13, 9, 8}, blob.getBytes(1, 7));

    MariaDbBlob blob2 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5});
    OutputStream out2 = blob2.setBinaryStream(4);
    out2.write(otherBytes);
    assertArrayEquals(new byte[] {0, 1, 2, 10, 11, 12, 13}, blob2.getBytes(1, 7));

    MariaDbBlob blob3 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out3 = blob3.setBinaryStream(2);
    out3.write(otherBytes);
    assertArrayEquals(new byte[] {2, 10, 11, 12, 13, 0, 0}, blob3.getBytes(1, 7));

    MariaDbBlob blob4 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out4 = blob4.setBinaryStream(4);
    out4.write(otherBytes);
    assertArrayEquals(new byte[] {2, 3, 4, 10, 11, 12}, blob4.getBytes(1, 6));

    MariaDbBlob blob5 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    Common.assertThrowsContains(
        SQLException.class, () -> blob5.setBinaryStream(0), "Invalid position in blob");
    Common.assertThrowsContains(
        IOException.class,
        () -> blob5.setBinaryStream(2).write(new byte[] {1}, 0, -5),
        "Invalid len -5");
    Common.assertThrowsContains(
        IOException.class,
        () -> blob5.setBinaryStream(2).write(new byte[] {1}, -2, 1),
        "Invalid offset -2");
  }

  @Test
  public void setBinaryStreamOffset() throws SQLException, IOException {
    final byte[] otherBytes = new byte[] {10, 11, 12, 13};

    MariaDbBlob blob = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5});
    OutputStream out = blob.setBinaryStream(2);
    out.write(otherBytes, 2, 3);
    assertArrayEquals(new byte[] {0, 12, 13, 3, 4, 5}, blob.getBytes(1, 6));

    MariaDbBlob blob2 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5});
    OutputStream out2 = blob2.setBinaryStream(4);
    out2.write(otherBytes, 3, 2);
    assertArrayEquals(new byte[] {0, 1, 2, 13, 4, 5, 0}, blob2.getBytes(1, 7));

    MariaDbBlob blob3 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 4);
    OutputStream out3 = blob3.setBinaryStream(2);
    out3.write(otherBytes, 2, 3);
    assertArrayEquals(new byte[] {2, 12, 13, 5, 0, 0, 0}, blob3.getBytes(1, 7));

    MariaDbBlob blob4 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out4 = blob4.setBinaryStream(4);
    out4.write(otherBytes, 2, 2);
    assertArrayEquals(new byte[] {2, 3, 4, 12, 13, 0}, blob4.getBytes(1, 6));

    MariaDbBlob blob5 = new MariaDbBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out5 = blob5.setBinaryStream(4);
    out5.write(otherBytes, 2, 20);
    assertArrayEquals(new byte[] {2, 3, 4, 12, 13, 0}, blob5.getBytes(1, 6));
  }

  @Test
  public void truncate() throws SQLException {
    MariaDbBlob blob = new MariaDbBlob(bytes);
    blob.truncate(20);
    assertArrayEquals(bytes, blob.getBytes(1, 6));
    blob.truncate(-5);
    assertArrayEquals(bytes, blob.getBytes(1, 6));
    blob.truncate(5);
    assertArrayEquals(new byte[] {0, 1, 2, 3, 4, 0, 0}, blob.getBytes(1, 7));
    blob.truncate(0);
    assertArrayEquals(new byte[] {0, 0}, blob.getBytes(1, 2));

    MariaDbBlob blob2 = new MariaDbBlob(bytes, 2, 3);
    blob2.truncate(20);
    assertArrayEquals(new byte[] {2, 3, 4}, blob2.getBytes(1, 3));
    blob2.truncate(2);
    assertArrayEquals(new byte[] {2, 3, 0, 0, 0, 0}, blob2.getBytes(1, 6));

    blob2.truncate(1);
    assertArrayEquals(new byte[] {2, 0}, blob2.getBytes(1, 2));
  }

  @Test
  public void free() {
    MariaDbBlob blob = new MariaDbBlob(bytes);
    blob.free();
    assertEquals(0, blob.length());
  }

  @Test
  public void expectedErrors() {
    Common.assertThrowsContains(
        IllegalArgumentException.class, () -> new MariaDbBlob(null), "byte array is null");
    Common.assertThrowsContains(
        IllegalArgumentException.class, () -> new MariaDbBlob(null, 0, 2), "byte array is null");
  }

  @Test
  public void equal() {
    MariaDbBlob blob = new MariaDbBlob(bytes);
    assertEquals(blob, blob);
    assertEquals(new MariaDbBlob(bytes), blob);
    assertNotEquals(null, blob);
    assertNotEquals("", blob);
    byte[] bytes = new byte[] {5, 1, 2, 3, 4, 5};
    assertNotEquals(new MariaDbBlob(bytes), blob);
    assertNotEquals(new MariaDbBlob(new byte[] {5, 1}), blob);
  }

  @Test
  public void hashCodeTest() {
    MariaDbBlob blob = new MariaDbBlob(bytes);
    assertEquals(-859797942, blob.hashCode());
  }
}
