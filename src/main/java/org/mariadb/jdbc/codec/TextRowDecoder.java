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

import java.sql.SQLException;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class TextRowDecoder extends RowDecoder {

  public TextRowDecoder(int columnCount, ColumnDefinitionPacket[] columns, Configuration conf) {
    super(columnCount, columns, conf);
  }

  @Override
  public <T> T decode(Codec<T> codec, Calendar cal) throws SQLException {
    return codec.decodeText(readBuf, length, columns[index], cal);
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
      readBuf.pos(0);
    } else {
      index++;
    }

    while (index < newIndex) {
      short type = this.readBuf.readUnsignedByte();
      switch (type) {
        case 252:
          readBuf.skip(readBuf.readUnsignedShort());
          break;
        case 253:
          readBuf.skip(readBuf.readUnsignedMedium());
          break;
        case 254:
          readBuf.skip((int) (4 + readBuf.readUnsignedInt()));
          break;
        case 251:
          break;
        default:
          readBuf.skip(type);
          break;
      }
      index++;
    }

    short type = this.readBuf.readUnsignedByte();
    switch (type) {
      case 251:
        length = NULL_LENGTH;
        break;
      case 252:
        length = readBuf.readUnsignedShort();
        break;
      case 253:
        length = readBuf.readUnsignedMedium();
        break;
      case 254:
        length = (int) readBuf.readUnsignedInt();
        readBuf.skip(4);
        break;
      default:
        length = type;
        break;
    }
  }
}
