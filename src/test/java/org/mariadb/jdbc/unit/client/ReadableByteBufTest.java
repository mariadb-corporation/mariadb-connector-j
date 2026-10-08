// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.ReadableByteBuf;

/**
 * Server-declared lengths must be validated against the packet before any allocation or copy: a
 * rogue server must not be able to drive a multi-gigabyte allocation from a few bytes.
 */
public class ReadableByteBufTest {

  // 0xFE + 8 bytes little-endian 0x7FFFFFFF
  private static final byte[] HUGE_LEN = {(byte) 254, -1, -1, -1, 127, 0, 0, 0, 0};
  // 0xFE + 8 bytes little-endian 0xFFFFFFFF (narrows to -1)
  private static final byte[] NEG_LEN = {(byte) 254, -1, -1, -1, -1, 0, 0, 0, 0};

  private static ReadableByteBuf buf(byte[] arr) {
    return new ReadableByteBuf(arr, arr.length);
  }

  @Test
  public void readLengthRejectsValuesNotFittingInt() {
    assertThrows(IllegalArgumentException.class, () -> buf(NEG_LEN).readLength());
    assertThrows(
        IllegalArgumentException.class,
        () -> buf(new byte[] {(byte) 254, 0, 0, 0, 0, 1, 0, 0, 0}).readLength());
    assertEquals(Integer.MAX_VALUE, buf(HUGE_LEN).readLength());
    assertNull(buf(new byte[] {(byte) 251}).readLength());
    assertEquals(5, buf(new byte[] {5}).readLength());
  }

  @Test
  public void readLengthBufferRejectsLengthBeyondPacket() {
    // declares a 5-byte sub-packet but only 2 bytes follow
    ReadableByteBuf b = buf(new byte[] {5, 1, 2});
    assertThrows(IllegalArgumentException.class, b::readLengthBuffer);

    byte[] huge = new byte[HUGE_LEN.length + 2];
    System.arraycopy(HUGE_LEN, 0, huge, 0, HUGE_LEN.length);
    assertThrows(IllegalArgumentException.class, () -> buf(huge).readLengthBuffer());

    ReadableByteBuf ok = buf(new byte[] {2, 7, 8, 9});
    ReadableByteBuf sub = ok.readLengthBuffer();
    assertEquals(2, sub.readableBytes());
    assertEquals(7, sub.readByte());
    assertEquals(8, sub.readByte());
    assertEquals(1, ok.readableBytes());
  }
}
