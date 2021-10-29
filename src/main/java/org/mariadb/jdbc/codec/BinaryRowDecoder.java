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

public class BinaryRowDecoder extends RowDecoder {

  private byte[] nullBitmap;

  public BinaryRowDecoder(int columnCount, Column[] columns, Configuration conf) {
    super(columnCount, columns, conf);
  }

  @Override
  public <T> T decode(Codec<T> codec, Calendar cal) throws SQLException {
    return codec.decodeBinary(readBuf, length, columns[index], cal);
  }

  @Override
  public byte decodeByte() throws SQLException {
    return ByteCodec.INSTANCE.decodeBinaryByte(readBuf, length, columns[index]);
  }

  @Override
  public boolean decodeBoolean() throws SQLException {
    return BooleanCodec.INSTANCE.decodeBinaryBoolean(readBuf, length, columns[index]);
  }

  @Override
  public short decodeShort() throws SQLException {
    return ShortCodec.INSTANCE.decodeBinaryShort(readBuf, length, columns[index]);
  }

  @Override
  public int decodeInt() throws SQLException {
    return IntCodec.INSTANCE.decodeBinaryInt(readBuf, length, columns[index]);
  }

  @Override
  public long decodeLong() throws SQLException {
    return LongCodec.INSTANCE.decodeBinaryLong(readBuf, length, columns[index]);
  }

  @Override
  public float decodeFloat() throws SQLException {
    return FloatCodec.INSTANCE.decodeBinaryFloat(readBuf, length, columns[index]);
  }

  @Override
  public double decodeDouble() throws SQLException {
    return DoubleCodec.INSTANCE.decodeBinaryDouble(readBuf, length, columns[index]);
  }

  @Override
  public void setRow(byte[] buf) {
    if (buf != null) {
      this.readBuf.buf(buf, buf.length).pos(1); // skip 0x00 header
      nullBitmap = new byte[(columnCount + 9) / 8];
      this.readBuf.readBytes(nullBitmap).mark();
    } else {
      this.readBuf.buf(null, 0);
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
      readBuf.reset();
    } else {
      index++;
    }

    while (index < newIndex) {
      if ((nullBitmap[(index + 2) / 8] & (1 << ((index + 2) % 8))) == 0) {
        // skip bytes
        switch (columns[index].getType()) {
          case BIGINT:
          case DOUBLE:
            readBuf.skip(8);
            break;

          case INTEGER:
          case MEDIUMINT:
          case FLOAT:
            readBuf.skip(4);
            break;

          case SMALLINT:
          case YEAR:
            readBuf.skip(2);
            break;

          case TINYINT:
            readBuf.skip(1);
            break;

          default:
            int type = this.readBuf.readUnsignedByte();
            switch (type) {
              case 252:
                this.readBuf.skip(this.readBuf.readUnsignedShort());
                break;

              case 253:
                this.readBuf.skip(this.readBuf.readUnsignedMedium());
                break;

              case 254:
                this.readBuf.skip((int) this.readBuf.readLong());
                break;

              default:
                this.readBuf.skip(type);
                break;
            }
            break;
        }
      }
      index++;
    }

    if ((nullBitmap[(index + 2) / 8] & (1 << ((index + 2) % 8))) > 0) {
      length = NULL_LENGTH;
      return;
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
        int len = this.readBuf.readUnsignedByte();
        switch (len) {
          case 252:
            // length is encoded on 3 bytes (0xfc header + 2 bytes indicating length)
            length = this.readBuf.readUnsignedShort();
            return;

          case 253:
            // length is encoded on 4 bytes (0xfd header + 3 bytes indicating length)
            length = this.readBuf.readUnsignedMedium();
            return;

          case 254:
            // length is encoded on 9 bytes (0xfe header + 8 bytes indicating length)
            length = (int) this.readBuf.readLong();
            return;

          default:
            // length is encoded on 1 bytes (is then less than 251)
            length = len;
        }
    }
  }
}
