// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.unit.client.socket;

import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.socket.Writer;

public class PacketWriterTest {

  @Test
  public void growBuffer() throws IOException {
    Writer pw = new Writer(null, 0, 0xffffff, null, null);
    Assertions.assertEquals(4, pw.pos());
    pw.writeBytes(new byte[8190], 0, 8190);
    pw.writeAscii("abcdefghij");
    Assertions.assertEquals(8200, pw.pos() - 4);

    for (int i = 0; i < 8190; i++) {
      Assertions.assertEquals(0, pw.buf()[i + 4]);
    }
    for (int i = 0; i < 10; i++) {
      Assertions.assertEquals('a' + i, pw.buf()[i + 8194]);
    }
  }

  private static byte[] written(Writer pw) {
    return java.util.Arrays.copyOfRange(pw.buf(), 4, pw.pos());
  }

  private static String escape(String str, boolean noBackslashEscapes) {
    if (noBackslashEscapes) return str.replace("'", "''");
    return str.replace("\\", "\\\\").replace("'", "\\'").replace("\0", "\\\0");
  }

  @Test
  public void writeStringAllLengths() throws IOException {
    // short strings use the char loop, long ones the JDK encoder: same bytes either way
    for (String base :
        new String[] {
          "ab", "select 1", "caf\u00e9 cr\u00e8me", "\u65e5\u672c\u8a9e \ud83d\ude00 mixed"
        }) {
      for (int repeat : new int[] {1, 4, 20, 500}) {
        String str = base.repeat(repeat);
        Writer pw = new Writer(null, 0, 0xffffff, null, null);
        pw.writeString(str);
        Assertions.assertArrayEquals(
            str.getBytes(java.nio.charset.StandardCharsets.UTF_8),
            written(pw),
            str.length() + " chars");
      }
    }
  }

  @Test
  public void writeStringEscapedAllLengths() throws IOException {
    String[] bases = {
      "plain",
      "it's",
      "back\\slash",
      "dbl\"quote",
      "nul\u0000char",
      "caf\u00e9 'cr\u00e8me'",
      "\u65e5\u672c\u8a9e \ud83d\ude00 \"mixed\"",
      "no escape needed at all in this string"
    };
    for (boolean noBackslashEscapes : new boolean[] {false, true}) {
      for (String base : bases) {
        for (int repeat : new int[] {1, 3, 10, 300}) {
          String str = base.repeat(repeat);
          Writer pw = new Writer(null, 0, 0xffffff, null, null);
          pw.writeStringEscaped(str, noBackslashEscapes);
          Assertions.assertArrayEquals(
              escape(str, noBackslashEscapes).getBytes(java.nio.charset.StandardCharsets.UTF_8),
              written(pw),
              (noBackslashEscapes ? "NO_BACKSLASH_ESCAPES " : "")
                  + str.length()
                  + " chars of "
                  + base);
        }
      }
    }
  }
}
