/*
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

package org.mariadb.jdbc.codec;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class TextRowDecoder extends RowDecoder {

  public TextRowDecoder(int columnCount, ColumnDefinitionPacket[] columns, Configuration conf) {
    super(columnCount, columns, conf);
  }

  /**
   * Get value.
   *
   * @param index REAL index (0 = first)
   * @param codec codec
   * @return value
   * @throws SQLException if cannot decode value
   */
  @Override
  public <T> T getValue(int index, Codec<T> codec, Calendar cal) throws SQLException {
    if (index < 1 || index > columnCount) {
      throw new SQLException(
          String.format(
              "Wrong index position. Is %s but must be in 1-%s range", index, columnCount));
    }
    if (buf == null) {
      throw new SQLDataException("wrong row position", "22023");
    }
    ColumnDefinitionPacket column = columns[index - 1];

    setPosition(index - 1);
    if (length == NULL_LENGTH) {
      return null;
    }
    return codec.decodeText(buf, length, column, cal);
  }

  @Override
  public <T> T decode(Codec<T> codec, Calendar cal) throws SQLException {
    return codec.decodeText(buf, length, columns[index], cal);
  }

  @Override
  public boolean wasNull() {
    return length == NULL_LENGTH;
  }

  /**
   * Set length and pos indicator to asked index.
   *
   * @param newIndex index (0 is first).
   */
  @Override
  public void setPosition(int newIndex) {
    if (index >= newIndex) {
      index = 0;
      buf.pos(0);
    } else {
      index++;
    }

    while (index < newIndex) {
      int type = this.buf.readUnsignedByte();
      switch (type) {
        case 252:
          buf.skip(buf.readUnsignedShort());
          break;
        case 253:
          buf.skip(buf.readUnsignedMedium());
          break;
        case 254:
          buf.skip((int) (4 + buf.readUnsignedInt()));
          break;
        case 251:
          break;
        default:
          buf.skip(type);
          break;
      }
      index++;
    }

    short type = this.buf.readUnsignedByte();
    switch (type) {
      case 251:
        length = NULL_LENGTH;
        break;
      case 252:
        length = buf.readUnsignedShort();
        break;
      case 253:
        length = buf.readUnsignedMedium();
        break;
      case 254:
        length = (int) buf.readUnsignedInt();
        buf.skip(4);
        break;
      default:
        length = type;
        break;
    }
  }
}
