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
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class BinaryRowDecoder extends RowDecoder {

  private byte[] nullBitmap;

  public BinaryRowDecoder(int columnCount, ColumnDefinitionPacket[] columns, Configuration conf) {
    super(columnCount, columns, conf);
  }

  /**
   * Get value.
   *
   * @param newIndex REAL index (0 = first)
   * @param codec codec
   * @return value
   * @throws SQLException if cannot decode value
   */
  @SuppressWarnings("unchecked")
  @Override
  public <T> T getValue(int newIndex, Codec<T> codec, Calendar cal) throws SQLException {
    if (newIndex < 1 || newIndex > columnCount) {
      throw new SQLException(
          String.format(
              "Wrong index position. Is %s but must be in 1-%s range", newIndex, columnCount));
    }
    if (buf == null) {
      throw new SQLDataException("wrong row position", "22023");
    }
    // check NULL-Bitmap that indicate if field is null
    if ((nullBitmap[(newIndex + 1) / 8] & (1 << ((newIndex + 1) % 8))) != 0) {
      index = newIndex - 1;
      return null;
    }
    setPosition(newIndex - 1);
    return codec.decodeBinary(buf, length, columns[index], cal);
  }

  @Override
  public <T> T decode(Codec<T> codec, Calendar cal) throws SQLException {
    return codec.decodeBinary(buf, length, columns[index], cal);
  }

  @Override
  public void setRow(ReadableByteBuf buf) {
    if (buf != null) {
      this.buf = buf;
      buf.pos(1); // skip 0x00 header
      nullBitmap = new byte[(columnCount + 9) / 8];
      buf.readBytes(nullBitmap);
      this.buf.mark();
    } else {
      this.buf = null;
    }
    index = -1;
  }

  @Override
  public boolean wasNull() {
    return (nullBitmap[(index + 2) / 8] & (1 << ((index + 2) % 8))) > 0;
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
      buf.reset();
    } else {
      index++;
    }

    while (index < newIndex) {
      if ((nullBitmap[(index + 2) / 8] & (1 << ((index + 2) % 8))) == 0) {
        // skip bytes
        switch (columns[index].getType()) {
          case BIGINT:
          case DOUBLE:
            buf.skip(8);
            break;

          case INTEGER:
          case MEDIUMINT:
          case FLOAT:
            buf.skip(4);
            break;

          case SMALLINT:
          case YEAR:
            buf.skip(2);
            break;

          case TINYINT:
            buf.skip(1);
            break;

          default:
            int type = this.buf.readUnsignedByte();
            switch (type) {
              case 251:
                break;

              case 252:
                this.buf.skip(this.buf.readUnsignedShort());
                break;

              case 253:
                this.buf.skip(this.buf.readUnsignedMedium());
                break;

              case 254:
                this.buf.skip((int) this.buf.readLong());
                break;

              default:
                this.buf.skip(type);
                break;
            }
            break;
        }
      }
      index++;
    }

    // read asked field position and length
    switch (columns[index].getType()) {
      case BIGINT:
      case DOUBLE:
        length = 8;
        return;

      case INTEGER:
      case MEDIUMINT:
      case FLOAT:
        length = 4;
        return;

      case SMALLINT:
      case YEAR:
        length = 2;
        return;

      case TINYINT:
        length = 1;
        return;

      default:
        // field with variable length
        int len = this.buf.readUnsignedByte();
        switch (len) {
          case 251:
            // null length field
            // must never occur
            // null value are set in NULL-Bitmap, not send with a null length indicator.
            throw new IllegalStateException(
                "null data is encoded in binary protocol but NULL-Bitmap is not set");

          case 252:
            // length is encoded on 3 bytes (0xfc header + 2 bytes indicating length)
            length = this.buf.readUnsignedShort();
            return;

          case 253:
            // length is encoded on 4 bytes (0xfd header + 3 bytes indicating length)
            length = this.buf.readUnsignedMedium();
            return;

          case 254:
            // length is encoded on 9 bytes (0xfe header + 8 bytes indicating length)
            length = (int) this.buf.readLong();
            return;

          default:
            // length is encoded on 1 bytes (is then less than 251)
            length = len;
            return;
        }
    }
  }
}
