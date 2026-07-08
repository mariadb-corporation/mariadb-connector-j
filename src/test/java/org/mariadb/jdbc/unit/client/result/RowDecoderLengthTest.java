// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.client.result;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.client.result.rowdecoder.BinaryRowDecoder;
import org.mariadb.jdbc.client.result.rowdecoder.TextRowDecoder;
import org.mariadb.jdbc.client.util.MutableInt;

public class RowDecoderLengthTest {

  private static final ColumnDecoder[] META = {ColumnDecoder.create("col", DataType.VARCHAR, 0)};

  @Test
  public void textOversizeLengthEncodedFieldRejected() {
    // a 0xFE length-encoded field whose low 32 bits are 0xFFFFFFFF used to narrow to -1
    // (NULL_LENGTH), silently turning a non-null value into SQL NULL
    byte[] row = {(byte) 254, -1, -1, -1, -1, 0, 0, 0, 0};
    StandardReadableByteBuf buf = new StandardReadableByteBuf(row, row.length);
    assertThrows(
        SQLException.class,
        () -> new TextRowDecoder().setPosition(0, new MutableInt(), 1, buf, new byte[0], META));
  }

  @Test
  public void textValidLengthEncodedFieldReturned() throws SQLException {
    byte[] row = {(byte) 254, 100, 0, 0, 0, 0, 0, 0, 0};
    StandardReadableByteBuf buf = new StandardReadableByteBuf(row, row.length);
    assertEquals(
        100, new TextRowDecoder().setPosition(0, new MutableInt(), 1, buf, new byte[0], META));
  }

  @Test
  public void binaryOversizeLengthEncodedFieldRejected() {
    // header + null-bitmap(1 byte, field not null) + 0xFE + 8-byte length
    byte[] row = {0, 0, (byte) 254, -1, -1, -1, -1, 0, 0, 0, 0};
    StandardReadableByteBuf buf = new StandardReadableByteBuf(row, row.length);
    assertThrows(
        SQLException.class,
        () -> new BinaryRowDecoder().setPosition(0, new MutableInt(), 1, buf, new byte[1], META));
  }

  @Test
  public void binaryValidLengthEncodedFieldReturned() throws SQLException {
    byte[] row = {0, 0, (byte) 254, 100, 0, 0, 0, 0, 0, 0, 0};
    StandardReadableByteBuf buf = new StandardReadableByteBuf(row, row.length);
    assertEquals(
        100, new BinaryRowDecoder().setPosition(0, new MutableInt(), 1, buf, new byte[1], META));
  }
}
