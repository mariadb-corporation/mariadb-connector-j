// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.unit.client.result;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.result.rowdecoder.BinaryRowDecoder;
import org.mariadb.jdbc.client.result.rowdecoder.TextRowDecoder;
import org.mariadb.jdbc.client.util.MutableInt;

public class RowDecoderLengthTest {

  private static final ColumnDecoder[] META = {
    ColumnDecoder.create("db", "col", DataType.VARCHAR, 0)
  };

  @Test
  public void textOversizeLengthEncodedFieldRejected() {
    // a 0xFE length-encoded field whose low 32 bits are 0xFFFFFFFF used to narrow to -1
    // (NULL_LENGTH), silently turning a non-null value into SQL NULL
    byte[] row = {(byte) 254, -1, -1, -1, -1, 0, 0, 0, 0};
    ReadableByteBuf buf = new ReadableByteBuf(row, row.length);
    assertThrows(
        SQLException.class,
        () -> new TextRowDecoder().setPosition(0, new MutableInt(), 1, buf, new byte[0], META));
  }

  @Test
  public void textValidLengthEncodedFieldReturned() throws SQLException {
    // 0xFE + 8-byte length 100, followed by the 100 bytes it declares
    byte[] row = new byte[9 + 100];
    row[0] = (byte) 254;
    row[1] = 100;
    ReadableByteBuf buf = new ReadableByteBuf(row, row.length);
    assertEquals(
        100, new TextRowDecoder().setPosition(0, new MutableInt(), 1, buf, new byte[0], META));
  }

  @Test
  public void binaryOversizeLengthEncodedFieldRejected() {
    // header + null-bitmap(1 byte, field not null) + 0xFE + 8-byte length
    byte[] row = {0, 0, (byte) 254, -1, -1, -1, -1, 0, 0, 0, 0};
    ReadableByteBuf buf = new ReadableByteBuf(row, row.length);
    assertThrows(
        SQLException.class,
        () -> new BinaryRowDecoder().setPosition(0, new MutableInt(), 1, buf, new byte[1], META));
  }

  @Test
  public void binaryValidLengthEncodedFieldReturned() throws SQLException {
    // header + null-bitmap + 0xFE + 8-byte length 100, followed by the 100 bytes it declares
    byte[] row = new byte[2 + 9 + 100];
    row[2] = (byte) 254;
    row[3] = 100;
    ReadableByteBuf buf = new ReadableByteBuf(row, row.length);
    assertEquals(
        100, new BinaryRowDecoder().setPosition(0, new MutableInt(), 1, buf, new byte[1], META));
  }

  @Test
  public void textLengthExceedingRowPacketRejected() {
    // 0xFE + 0x7FFFFFFF fits an int but not the packet: column decoders would allocate 2 GB
    byte[] row = {(byte) 254, -1, -1, -1, 127, 0, 0, 0, 0, 1, 2};
    ReadableByteBuf buf = new ReadableByteBuf(row, row.length);
    assertThrows(
        SQLException.class,
        () -> new TextRowDecoder().setPosition(0, new MutableInt(), 1, buf, new byte[0], META));

    // 1-byte form: declares 5 bytes, only 2 present
    byte[] shortRow = {5, 'a', 'b'};
    ReadableByteBuf buf2 = new ReadableByteBuf(shortRow, shortRow.length);
    assertThrows(
        SQLException.class,
        () -> new TextRowDecoder().setPosition(0, new MutableInt(), 1, buf2, new byte[0], META));

    // exact fit is accepted
    byte[] exact = {2, 'a', 'b'};
    ReadableByteBuf buf3 = new ReadableByteBuf(exact, exact.length);
    assertDoesNotThrow(
        () -> new TextRowDecoder().setPosition(0, new MutableInt(), 1, buf3, new byte[0], META));
  }

  @Test
  public void binaryLengthExceedingRowPacketRejected() {
    byte[] row = {0, 0, (byte) 254, -1, -1, -1, 127, 0, 0, 0, 0, 1, 2};
    ReadableByteBuf buf = new ReadableByteBuf(row, row.length);
    assertThrows(
        SQLException.class,
        () -> new BinaryRowDecoder().setPosition(0, new MutableInt(), 1, buf, new byte[1], META));

    byte[] shortRow = {0, 0, 5, 'a', 'b'};
    ReadableByteBuf buf2 = new ReadableByteBuf(shortRow, shortRow.length);
    assertThrows(
        SQLException.class,
        () -> new BinaryRowDecoder().setPosition(0, new MutableInt(), 1, buf2, new byte[1], META));

    // fixed-size type truncated: BIGINT needs 8 bytes, row has 3
    ColumnDecoder[] bigint = {ColumnDecoder.create("db", "col", DataType.BIGINT, 0)};
    byte[] truncated = {0, 0, 1, 2, 3};
    ReadableByteBuf buf3 = new ReadableByteBuf(truncated, truncated.length);
    assertThrows(
        SQLException.class,
        () ->
            new BinaryRowDecoder().setPosition(0, new MutableInt(), 1, buf3, new byte[1], bigint));
  }
}
