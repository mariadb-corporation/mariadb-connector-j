// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Common;
import com.singlestore.jdbc.SingleStoreBlob;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

public class BlobTest extends Common {

  private final byte[] bytes = new byte[] {0, 1, 2, 3, 4, 5};

  @Test
  public void length() throws SQLException {
    SingleStoreBlob blob = new SingleStoreBlob(bytes);
    assertEquals(6, blob.length());

    SingleStoreBlob blob2 = new SingleStoreBlob(bytes, 2, 3);
    assertEquals(3, blob2.length());
  }

  @Test
  public void getBytes() throws SQLException {
    SingleStoreBlob blob = new SingleStoreBlob(bytes);
    assertArrayEquals(bytes, blob.getBytes(1, 6));
    assertArrayEquals(new byte[] {0, 1, 2, 3, 4, 5, 0}, blob.getBytes(1, 7));
    assertArrayEquals(new byte[] {0, 1}, blob.getBytes(1, 2));

    SingleStoreBlob blob2 = new SingleStoreBlob(bytes, 2, 3);
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
    SingleStoreBlob blob = new SingleStoreBlob(bytes);
    assureInputStreamEqual(bytes, blob.getBinaryStream(1, 6));
    assertThrowsContains(
        SQLException.class,
        () -> assureInputStreamEqual(new byte[] {0, 1, 2, 3, 4, 5, 0}, blob.getBinaryStream(1, 7)),
        "Out of range (position + length - 1 > streamSize)");
    assertThrowsContains(
        SQLException.class,
        () -> assureInputStreamEqual(new byte[] {0, 1, 2, 3, 4, 5, 0}, blob.getBinaryStream(-2, 7)),
        "Out of range (position should be > 0)");
    assertThrowsContains(
        SQLException.class,
        () -> assureInputStreamEqual(new byte[] {0, 1, 2, 3, 4, 5, 0}, blob.getBinaryStream(20, 7)),
        "Out of range (position > stream size)");

    assureInputStreamEqual(new byte[] {0, 1}, blob.getBinaryStream(1, 2));

    SingleStoreBlob blob2 = new SingleStoreBlob(bytes, 2, 3);
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
    SingleStoreBlob blob = new SingleStoreBlob(bytes);
    assertEquals(5, blob.position(new byte[] {4, 5}, 2));

    SingleStoreBlob blob2 = new SingleStoreBlob(bytes, 2, 4);
    assertEquals(3, blob2.position(new byte[] {4, 5}, 2));
    assertEquals(0, blob2.position(new byte[0], 2));
    assertEquals(-1, blob2.position(new byte[] {4, 9}, 2));

    assertEquals(3, blob2.position(new SingleStoreBlob(new byte[] {4, 5}), 2));

    assertThrowsContains(
        SQLException.class,
        () -> blob2.position(new byte[] {4, 5}, -2),
        "Out of range (position should be > 0, " + "but is -2)");
    assertThrowsContains(
        SQLException.class,
        () -> blob2.position(new byte[] {4, 5}, 20),
        "Out of range (start > stream size)");
  }

  @Test
  public void setBytes() throws SQLException {
    final byte[] bytes = new byte[] {0, 1, 2, 3, 4, 5};
    final byte[] otherBytes = new byte[] {10, 11, 12, 13};

    SingleStoreBlob blob = new SingleStoreBlob(new byte[0]);
    blob.setBytes(1, new byte[0]);
    assertEquals(0, blob.length());
    blob.setBytes(1, new byte[0], 0, 0);
    assertEquals(0, blob.length());

    blob = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5});
    blob.setBytes(2, otherBytes);
    assertArrayEquals(new byte[] {0, 10, 11, 12, 13, 5}, blob.getBytes(1, 6));

    SingleStoreBlob blob2 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5});
    blob2.setBytes(4, otherBytes);
    assertArrayEquals(new byte[] {0, 1, 2, 10, 11, 12, 13}, blob2.getBytes(1, 7));

    SingleStoreBlob blob3 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    blob3.setBytes(2, otherBytes);
    assertArrayEquals(new byte[] {2, 10, 11, 12, 13, 0, 0}, blob3.getBytes(1, 7));

    SingleStoreBlob blob4 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    blob4.setBytes(4, otherBytes);
    assertArrayEquals(new byte[] {2, 3, 4, 10, 11, 12}, blob4.getBytes(1, 6));

    try {
      SingleStoreBlob blob5 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
      blob5.setBytes(0, otherBytes);
    } catch (SQLException sqle) {
      // normal exception
    }
  }

  @Test
  public void setBytesOffset() throws SQLException {
    final byte[] bytes = new byte[] {0, 1, 2, 3, 4, 5};
    final byte[] otherBytes = new byte[] {10, 11, 12, 13};

    SingleStoreBlob blob = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5});
    blob.setBytes(2, otherBytes, 2, 3);
    assertArrayEquals(new byte[] {0, 12, 13, 3, 4, 5}, blob.getBytes(1, 6));

    SingleStoreBlob blob2 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5});
    blob2.setBytes(4, otherBytes, 3, 2);
    assertArrayEquals(new byte[] {0, 1, 2, 13, 4, 5, 0}, blob2.getBytes(1, 7));

    SingleStoreBlob blob3 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 4);
    blob3.setBytes(2, otherBytes, 2, 3);
    assertArrayEquals(new byte[] {2, 12, 13, 5, 0, 0, 0}, blob3.getBytes(1, 7));

    SingleStoreBlob blob4 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    blob4.setBytes(4, otherBytes, 2, 2);
    assertArrayEquals(new byte[] {2, 3, 4, 12, 13, 0}, blob4.getBytes(1, 6));

    SingleStoreBlob blob5 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    blob5.setBytes(4, otherBytes, 2, 20);
    assertArrayEquals(new byte[] {2, 3, 4, 12, 13, 0}, blob5.getBytes(1, 6));

    try {
      SingleStoreBlob blob6 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
      blob6.setBytes(0, otherBytes, 2, 3);
    } catch (SQLException sqle) {
      // normal exception
    }
  }

  @Test
  public void setBinaryStream() throws SQLException, IOException {
    final byte[] bytes = new byte[] {0, 1, 2, 3, 4, 5};
    final byte[] otherBytes = new byte[] {10, 11, 12, 13};

    SingleStoreBlob blob = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5});
    OutputStream out = blob.setBinaryStream(2);
    out.write(otherBytes);
    out.write(0x09);
    out.write(0x08);
    assertArrayEquals(new byte[] {0, 10, 11, 12, 13, 9, 8}, blob.getBytes(1, 7));

    SingleStoreBlob blob2 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5});
    OutputStream out2 = blob2.setBinaryStream(4);
    out2.write(otherBytes);
    assertArrayEquals(new byte[] {0, 1, 2, 10, 11, 12, 13}, blob2.getBytes(1, 7));

    SingleStoreBlob blob3 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out3 = blob3.setBinaryStream(2);
    out3.write(otherBytes);
    assertArrayEquals(new byte[] {2, 10, 11, 12, 13, 0, 0}, blob3.getBytes(1, 7));

    SingleStoreBlob blob4 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out4 = blob4.setBinaryStream(4);
    out4.write(otherBytes);
    assertArrayEquals(new byte[] {2, 3, 4, 10, 11, 12}, blob4.getBytes(1, 6));

    SingleStoreBlob blob5 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    assertThrowsContains(
        SQLException.class, () -> blob5.setBinaryStream(0), "Invalid position in blob");
    assertThrowsContains(
        IOException.class,
        () -> blob5.setBinaryStream(2).write(new byte[] {1}, 0, -5),
        "Invalid len -5");
    assertThrowsContains(
        IOException.class,
        () -> blob5.setBinaryStream(2).write(new byte[] {1}, -2, 1),
        "Invalid offset -2");
  }

  @Test
  public void setBinaryStreamOffset() throws SQLException, IOException {
    final byte[] bytes = new byte[] {0, 1, 2, 3, 4, 5};
    final byte[] otherBytes = new byte[] {10, 11, 12, 13};

    SingleStoreBlob blob = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5});
    OutputStream out = blob.setBinaryStream(2);
    out.write(otherBytes, 2, 3);
    assertArrayEquals(new byte[] {0, 12, 13, 3, 4, 5}, blob.getBytes(1, 6));

    SingleStoreBlob blob2 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5});
    OutputStream out2 = blob2.setBinaryStream(4);
    out2.write(otherBytes, 3, 2);
    assertArrayEquals(new byte[] {0, 1, 2, 13, 4, 5, 0}, blob2.getBytes(1, 7));

    SingleStoreBlob blob3 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 4);
    OutputStream out3 = blob3.setBinaryStream(2);
    out3.write(otherBytes, 2, 3);
    assertArrayEquals(new byte[] {2, 12, 13, 5, 0, 0, 0}, blob3.getBytes(1, 7));

    SingleStoreBlob blob4 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out4 = blob4.setBinaryStream(4);
    out4.write(otherBytes, 2, 2);
    assertArrayEquals(new byte[] {2, 3, 4, 12, 13, 0}, blob4.getBytes(1, 6));

    SingleStoreBlob blob5 = new SingleStoreBlob(new byte[] {0, 1, 2, 3, 4, 5}, 2, 3);
    OutputStream out5 = blob5.setBinaryStream(4);
    out5.write(otherBytes, 2, 20);
    assertArrayEquals(new byte[] {2, 3, 4, 12, 13, 0}, blob5.getBytes(1, 6));
  }

  @Test
  public void truncate() throws SQLException {
    SingleStoreBlob blob = new SingleStoreBlob(bytes);
    blob.truncate(20);
    assertArrayEquals(bytes, blob.getBytes(1, 6));
    blob.truncate(-5);
    assertArrayEquals(bytes, blob.getBytes(1, 6));
    blob.truncate(5);
    assertArrayEquals(new byte[] {0, 1, 2, 3, 4, 0, 0}, blob.getBytes(1, 7));
    blob.truncate(0);
    assertArrayEquals(new byte[] {0, 0}, blob.getBytes(1, 2));

    SingleStoreBlob blob2 = new SingleStoreBlob(bytes, 2, 3);
    blob2.truncate(20);
    assertArrayEquals(new byte[] {2, 3, 4}, blob2.getBytes(1, 3));
    blob2.truncate(2);
    assertArrayEquals(new byte[] {2, 3, 0, 0, 0, 0}, blob2.getBytes(1, 6));

    blob2.truncate(1);
    assertArrayEquals(new byte[] {2, 0}, blob2.getBytes(1, 2));
  }

  @Test
  public void free() throws SQLException {
    SingleStoreBlob blob = new SingleStoreBlob(bytes);
    blob.free();
    assertEquals(0, blob.length());
  }

  @Test
  public void expectedErrors() {
    assertThrowsContains(
        IllegalArgumentException.class, () -> new SingleStoreBlob(null), "byte array is null");
    assertThrowsContains(
        IllegalArgumentException.class,
        () -> new SingleStoreBlob(null, 0, 2),
        "byte array is null");
  }

  @Test
  public void equal() {
    SingleStoreBlob blob = new SingleStoreBlob(bytes);
    assertEquals(blob, blob);
    assertEquals(new SingleStoreBlob(bytes), blob);
    assertFalse(blob.equals(null));
    assertFalse(blob.equals(""));
    byte[] bytes = new byte[] {5, 1, 2, 3, 4, 5};
    assertNotEquals(new SingleStoreBlob(bytes), blob);
    assertNotEquals(new SingleStoreBlob(new byte[] {5, 1}), blob);
  }

  @Test
  public void hashCodeTest() {
    SingleStoreBlob blob = new SingleStoreBlob(bytes);
    assertEquals(-859797942, blob.hashCode());
  }
}
