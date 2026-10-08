// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;

/**
 * Lengths declared inside the extended column metadata are server-controlled: they must be bounded
 * by the sub-packet before being used to read or skip.
 */
public class ColumnDecoderTest {

  // catalog "def", schema "s", table "t", table alias "t", column alias "c", column "c"
  private static final String IDENTIFIERS = "03 64 65 66 01 73 01 74 01 74 01 63 01 63";
  // fixed-length part: 0x0c, charset, column length, type, flags, decimals, filler
  private static final String TAIL = " 0C 3F 00 01 00 00 00 10 20 00 00 00 00";

  private static StandardReadableByteBuf packet(String extendedInfo) {
    String[] parts = (IDENTIFIERS + " " + extendedInfo + TAIL).trim().split("\\s+");
    byte[] def = new byte[parts.length];
    for (int i = 0; i < parts.length; i++) {
      def[i] = (byte) Integer.parseInt(parts[i], 16);
    }
    return new StandardReadableByteBuf(def, def.length);
  }

  @Test
  public void validExtendedTypeNameFillingTheSubPacket() {
    // 6-byte sub-packet: type 0, length 4, "uuid". The entry's data ends exactly where the
    // sub-packet ends, which is how a server sends it: it must be accepted.
    StandardReadableByteBuf buf = packet("06 00 04 75 75 69 64");
    assertEquals("uuid", ColumnDecoder.decode(buf).getExtTypeName());
  }

  @Test
  public void hugeExtendedTypeNameLengthRejected() {
    // 10-byte sub-packet holding type 0 and a 0x7FFFFFFF name length: must fail with a bounds
    // error before anything is read or allocated from that length
    StandardReadableByteBuf buf = packet("0A 00 FE FF FF FF 7F 00 00 00 00");
    assertThrows(IllegalArgumentException.class, () -> ColumnDecoder.decode(buf));
  }

  @Test
  public void negativeExtendedSkipLengthRejectedInsteadOfLooping() {
    // 10-byte sub-packet holding an unknown type 2 and a length whose low 32 bits are 0xFFFFFFF6
    // (-10 once narrowed). Unchecked, skip(-10) rewinds the sub-packet by exactly the bytes the
    // iteration consumed, and the loop never ends.
    StandardReadableByteBuf buf = packet("0A 02 FE F6 FF FF FF 00 00 00 00");
    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> assertThrows(IllegalArgumentException.class, () -> ColumnDecoder.decode(buf)));
  }

  @Test
  public void skipLengthExceedingSubPacketRejected() {
    // 4-byte sub-packet: unknown type 2 declaring 9 bytes with 2 present
    StandardReadableByteBuf buf = packet("04 02 09 AA BB");
    assertThrows(IllegalArgumentException.class, () -> ColumnDecoder.decode(buf));
  }
}
