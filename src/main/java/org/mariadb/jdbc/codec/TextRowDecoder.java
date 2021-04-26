// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.codec;

import java.sql.SQLException;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.codec.list.*;
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
  public byte decodeByte() throws SQLException {
    return ByteCodec.INSTANCE.decodeTextByte(readBuf, length, columns[index]);
  }

  @Override
  public boolean decodeBoolean() throws SQLException {
    return BooleanCodec.INSTANCE.decodeTextBoolean(readBuf, length, columns[index]);
  }

  @Override
  public short decodeShort() throws SQLException {
    return ShortCodec.INSTANCE.decodeTextShort(readBuf, length, columns[index]);
  }

  @Override
  public int decodeInt() throws SQLException {
    return IntCodec.INSTANCE.decodeTextInt(readBuf, length, columns[index]);
  }

  @Override
  public long decodeLong() throws SQLException {
    return LongCodec.INSTANCE.decodeTextLong(readBuf, length, columns[index]);
  }

  @Override
  public float decodeFloat() throws SQLException {
    return FloatCodec.INSTANCE.decodeTextFloat(readBuf, length, columns[index]);
  }

  @Override
  public double decodeDouble() throws SQLException {
    return DoubleCodec.INSTANCE.decodeTextDouble(readBuf, length, columns[index]);
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
