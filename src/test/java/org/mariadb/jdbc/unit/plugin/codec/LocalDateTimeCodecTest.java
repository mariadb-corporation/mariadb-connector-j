// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.plugin.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.codec.LocalDateTimeCodec;

public class LocalDateTimeCodecTest {

  // Europe/Vienna springs forward on 2024-03-31 at 02:00 -> 03:00, so 02:45 is a wall-clock time
  // that does not exist in that zone. DATETIME is zoneless, so it can still hold that value.
  private static final Calendar VIENNA =
      Calendar.getInstance(TimeZone.getTimeZone("Europe/Vienna"));

  private static ColumnDecoder datetimeColumn() {
    return ColumnDecoder.create("db", "dt", DataType.DATETIME, 0);
  }

  // CONJ-1264: a DATETIME value must be returned verbatim as LocalDateTime. Previously decode
  // round-tripped through ZonedDateTime.atZone(...), which shifts a DST-gap local time forward
  // (02:45 -> 03:45), so retrieving differed from the stored value.
  @Test
  public void decodeTextDatetimeInDstGap() throws Exception {
    byte[] bytes = "2024-03-31 02:45:00".getBytes(StandardCharsets.US_ASCII);
    ReadableByteBuf buf = new ReadableByteBuf(bytes, bytes.length);
    LocalDateTime res =
        LocalDateTimeCodec.INSTANCE.decodeText(
            buf, new MutableInt(bytes.length), datetimeColumn(), VIENNA, null);
    assertEquals(LocalDateTime.of(2024, 3, 31, 2, 45, 0), res);
  }

  @Test
  public void decodeBinaryDatetimeInDstGap() throws Exception {
    // binary DATETIME (length 7): year(2 LE)=2024, month=3, day=31, hour=2, minute=45, second=0
    byte[] bytes = new byte[] {(byte) 0xE8, 0x07, 0x03, 0x1F, 0x02, 0x2D, 0x00};
    ReadableByteBuf buf = new ReadableByteBuf(bytes, bytes.length);
    LocalDateTime res =
        LocalDateTimeCodec.INSTANCE.decodeBinary(
            buf, new MutableInt(bytes.length), datetimeColumn(), VIENNA, null);
    assertEquals(LocalDateTime.of(2024, 3, 31, 2, 45, 0), res);
  }

  // a value not in a DST transition must remain unchanged (guards against over-correction)
  @Test
  public void decodeTextDatetimeNormal() throws Exception {
    byte[] bytes = "2024-06-15 12:00:00".getBytes(StandardCharsets.US_ASCII);
    ReadableByteBuf buf = new ReadableByteBuf(bytes, bytes.length);
    LocalDateTime res =
        LocalDateTimeCodec.INSTANCE.decodeText(
            buf, new MutableInt(bytes.length), datetimeColumn(), VIENNA, null);
    assertEquals(LocalDateTime.of(2024, 6, 15, 12, 0, 0), res);
  }

  // the undocumented pureLocalDateTime option resolves once on the Configuration, default true.
  @Test
  public void pureLocalDateTimeOption() throws Exception {
    assertTrue(Configuration.parse("jdbc:mariadb://localhost/db").pureLocalDateTime());
    assertTrue(
        Configuration.parse("jdbc:mariadb://localhost/db?pureLocalDateTime=true")
            .pureLocalDateTime());
    assertFalse(
        Configuration.parse("jdbc:mariadb://localhost/db?pureLocalDateTime=false")
            .pureLocalDateTime());
  }
}
