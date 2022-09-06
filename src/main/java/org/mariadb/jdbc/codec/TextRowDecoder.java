// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.codec;

import java.sql.SQLException;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.Column;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.plugin.codec.*;

public class TextRowDecoder extends RowDecoder {

  public TextRowDecoder(int columnCount, Column[] columns, Configuration conf) {
    super(columnCount, columns, conf);
  }

  @Override
  public <T> T decode(Codec<T> codec, Calendar cal) throws SQLException {
    return codec.decodeText(readBuf, length, columns[index], cal);
  }

  @Override
  public String decodeString() throws SQLException {
    return StringCodec.INSTANCE.decodeText(readBuf, length, columns[index], null);
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
      this.readBuf.skipLengthEncoded();
      index++;
    }

    short len = this.readBuf.readUnsignedByte();
    if (len < 251) {
      // length is encoded on 1 bytes (is then less than 251)
      length = len;
      return;
    }
    switch (len) {
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
    }
  }
}
