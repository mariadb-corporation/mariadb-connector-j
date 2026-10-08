// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;

/**
 * Server-declared lengths are server-controlled: an 8-byte length-encoded value must fit a
 * non-negative int before being used as a length, otherwise a rogue server could drive a negative
 * or wrapped length into the packet parsing.
 */
public class ReadableByteBufTest {

  // 0xFE + 8 bytes little-endian 0x7FFFFFFF
  private static final byte[] HUGE_LEN = {(byte) 254, -1, -1, -1, 127, 0, 0, 0, 0};
  // 0xFE + 8 bytes little-endian 0xFFFFFFFF (narrows to -1 without the check)
  private static final byte[] NEG_LEN = {(byte) 254, -1, -1, -1, -1, 0, 0, 0, 0};
  // 0xFE + 8 bytes little-endian 0x100000000
  private static final byte[] OVER_INT_LEN = {(byte) 254, 0, 0, 0, 0, 1, 0, 0, 0};

  private static ReadableByteBuf buf(byte[] arr) {
    return new StandardReadableByteBuf(arr, arr.length);
  }

  @Test
  public void readLengthRejectsValuesNotFittingInt() {
    assertThrows(IllegalArgumentException.class, () -> buf(NEG_LEN).readLength());
    assertThrows(IllegalArgumentException.class, () -> buf(OVER_INT_LEN).readLength());
    assertEquals(Integer.MAX_VALUE, buf(HUGE_LEN).readLength());
    assertNull(buf(new byte[] {(byte) 251}).readLength());
    assertEquals(5, buf(new byte[] {5}).readLength());
    assertEquals(0x0201, buf(new byte[] {(byte) 252, 1, 2}).readLength());
    assertEquals(0x030201, buf(new byte[] {(byte) 253, 1, 2, 3}).readLength());
  }

  @Test
  public void readLengthBufferRejectsLengthBeyondPacket() {
    // declares a 5-byte sub-packet but only 2 bytes follow
    ReadableByteBuf b = buf(new byte[] {5, 1, 2});
    assertThrows(IllegalArgumentException.class, b::readLengthBuffer);

    byte[] huge = new byte[HUGE_LEN.length + 2];
    System.arraycopy(HUGE_LEN, 0, huge, 0, HUGE_LEN.length);
    assertThrows(IllegalArgumentException.class, () -> buf(huge).readLengthBuffer());

    // the sub-packet is bounded by the parent's limit, not by the backing array
    ReadableByteBuf reusable = new StandardReadableByteBuf(new byte[] {3, 1, 2, 3, 9, 9}, 3);
    assertThrows(IllegalArgumentException.class, reusable::readLengthBuffer);

    ReadableByteBuf ok = buf(new byte[] {2, 7, 8, 9});
    ReadableByteBuf sub = ok.readLengthBuffer();
    assertEquals(2, sub.readableBytes());
    assertEquals(7, sub.readByte());
    assertEquals(8, sub.readByte());
    assertEquals(1, ok.readableBytes());
  }

  @Test
  public void readIntLengthEncodedNotNullRejectsValuesNotFittingInt() {
    assertThrows(IllegalArgumentException.class, () -> buf(NEG_LEN).readIntLengthEncodedNotNull());
    assertThrows(
        IllegalArgumentException.class, () -> buf(OVER_INT_LEN).readIntLengthEncodedNotNull());
    assertEquals(Integer.MAX_VALUE, buf(HUGE_LEN).readIntLengthEncodedNotNull());
    assertEquals(5, buf(new byte[] {5}).readIntLengthEncodedNotNull());
    assertEquals(0x0201, buf(new byte[] {(byte) 252, 1, 2}).readIntLengthEncodedNotNull());
    assertEquals(0x030201, buf(new byte[] {(byte) 253, 1, 2, 3}).readIntLengthEncodedNotNull());
  }
}
