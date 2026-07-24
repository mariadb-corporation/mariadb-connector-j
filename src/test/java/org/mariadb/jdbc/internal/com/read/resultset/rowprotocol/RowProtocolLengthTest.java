/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.internal.com.read.resultset.rowprotocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.mariadb.jdbc.internal.ColumnType;
import org.mariadb.jdbc.internal.com.read.resultset.ColumnDefinition;
import org.mariadb.jdbc.util.Options;

/**
 * A length-encoded 0xFE field length is 8 bytes. Narrowing it to int with an unchecked cast let a
 * value ending 0xFFFFFFFF become -1 (NULL_LENGTH, turning a non-null value into SQL NULL) and other
 * high values become a negative length reaching new byte[]. The decoders now reject anything
 * outside 0..Integer.MAX_VALUE.
 */
public class RowProtocolLengthTest {

  @Test
  public void textOversizeLengthEncodedFieldRejected() {
    TextRowProtocol row = new TextRowProtocol(0, new Options());
    row.resetRow(new byte[] {(byte) 254, -1, -1, -1, -1, 0, 0, 0, 0});
    try {
      row.setPosition(0);
      fail("expected an exception for an oversize length-encoded field");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void textValidLengthEncodedFieldAccepted() {
    TextRowProtocol row = new TextRowProtocol(0, new Options());
    row.resetRow(new byte[] {(byte) 254, 100, 0, 0, 0, 0, 0, 0, 0});
    row.setPosition(0);
    assertEquals(100, row.length);
  }

  @Test
  public void binaryOversizeLengthEncodedFieldRejected() {
    ColumnDefinition[] cols = {ColumnDefinition.create("col", ColumnType.VARCHAR)};
    BinaryRowProtocol row = new BinaryRowProtocol(cols, 1, 0, new Options());
    // header + null-bitmap(1 byte, field not null) + 0xFE + 8-byte length
    row.resetRow(new byte[] {0, 0, (byte) 254, -1, -1, -1, -1, 0, 0, 0, 0});
    try {
      row.setPosition(0);
      fail("expected an exception for an oversize length-encoded field");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void binaryValidLengthEncodedFieldAccepted() {
    ColumnDefinition[] cols = {ColumnDefinition.create("col", ColumnType.VARCHAR)};
    BinaryRowProtocol row = new BinaryRowProtocol(cols, 1, 0, new Options());
    row.resetRow(new byte[] {0, 0, (byte) 254, 100, 0, 0, 0, 0, 0, 0, 0});
    row.setPosition(0);
    assertEquals(100, row.length);
  }
}
