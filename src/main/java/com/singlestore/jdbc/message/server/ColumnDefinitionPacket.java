// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.server;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.codec.Codec;
import com.singlestore.jdbc.codec.DataType;
import com.singlestore.jdbc.codec.list.*;
import com.singlestore.jdbc.util.CharsetEncodingLength;
import com.singlestore.jdbc.util.constants.ColumnFlags;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Objects;

public class ColumnDefinitionPacket implements ServerMessage {

  private final ReadableByteBuf buf;
  private final int charset;
  private final long length;
  private final DataType dataType;
  private final byte decimals;
  private final int flags;
  private final int[] stringPos;
  private final String extTypeName;
  private boolean useAliasAsName;

  private ColumnDefinitionPacket(
      ReadableByteBuf buf,
      int charset,
      long length,
      DataType dataType,
      byte decimals,
      int flags,
      int[] stringPos) {
    this.buf = buf;
    this.charset = charset;
    this.length = length;
    this.dataType = dataType;
    this.decimals = decimals;
    this.flags = flags;
    this.stringPos = stringPos;
    this.extTypeName = null;
  }

  public ColumnDefinitionPacket(ReadableByteBuf buf, boolean extendedInfo) {
    // skip first strings
    stringPos = new int[5];
    stringPos[0] = buf.skipIdentifier(); // schema pos
    stringPos[1] = buf.skipIdentifier(); // table alias pos
    stringPos[2] = buf.skipIdentifier(); // table pos
    stringPos[3] = buf.skipIdentifier(); // column alias pos
    stringPos[4] = buf.skipIdentifier(); // column pos
    buf.skipIdentifier();

    if (extendedInfo) {
      String tmpTypeName = null;

      // fast skipping extended info (usually not set)
      if (buf.readByte() != 0) {
        // revert position, because has extended info.
        buf.pos(buf.pos() - 1);

        ReadableByteBuf subPacket = buf.readLengthBuffer();
        while (subPacket.readableBytes() > 0) {
          if (subPacket.readByte() == 0) {
            tmpTypeName = subPacket.readAscii(subPacket.readLength());
          } else { // skip data
            subPacket.skip(subPacket.readLength());
          }
        }
      }
      extTypeName = tmpTypeName;
    } else {
      extTypeName = null;
    }

    this.buf = buf;
    buf.skip(); // skip length always 0x0c
    this.charset = buf.readShort();
    this.length = buf.readInt();
    this.dataType = DataType.of(buf.readUnsignedByte());
    this.flags = buf.readUnsignedShort();
    this.decimals = buf.readByte();
  }

  public static ColumnDefinitionPacket create(String name, DataType type) {
    byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    byte[] arr = new byte[9 + 2 * nameBytes.length];
    arr[0] = 3;
    arr[1] = 'D';
    arr[2] = 'E';
    arr[3] = 'F';

    int[] stringPos = new int[5];
    stringPos[0] = 4; // schema pos
    stringPos[1] = 5; // table alias pos
    stringPos[2] = 6; // table pos

    // lenenc_str     name
    // lenenc_str     org_name
    int pos = 7;
    for (int i = 0; i < 2; i++) {
      stringPos[i + 3] = pos;
      arr[pos++] = (byte) nameBytes.length;
      System.arraycopy(nameBytes, 0, arr, pos, nameBytes.length);
      pos += nameBytes.length;
    }
    int len;

    /* Sensible predefined length - since we're dealing with I_S here, most char fields are 64 char long */
    switch (type) {
      case VARCHAR:
      case VARSTRING:
        len = 64 * 3; /* 3 bytes per UTF8 char */
        break;
      case SMALLINT:
        len = 5;
        break;
      case NULL:
        len = 0;
        break;
      default:
        len = 1;
        break;
    }

    return new ColumnDefinitionPacket(
        new ReadableByteBuf(null, arr, arr.length),
        33,
        len,
        type,
        (byte) 0,
        ColumnFlags.PRIMARY_KEY,
        stringPos);
  }

  public String getSchema() {
    buf.pos(stringPos[0]);
    return buf.readString(buf.readLength());
  }

  public String getTableAlias() {
    buf.pos(stringPos[1]);
    return buf.readString(buf.readLength());
  }

  public String getTable() {
    buf.pos(stringPos[useAliasAsName ? 1 : 2]);
    return buf.readString(buf.readLength());
  }

  public String getColumnAlias() {
    buf.pos(stringPos[3]);
    return buf.readString(buf.readLength());
  }

  public String getColumn() {
    buf.pos(stringPos[4]);
    return buf.readString(buf.readLength());
  }

  public long getLength() {
    return length;
  }

  public DataType getType() {
    return dataType;
  }

  public byte getDecimals() {
    if (dataType == DataType.DATE) {
      return 0;
    }
    return decimals;
  }

  public boolean isSigned() {
    return (flags & ColumnFlags.UNSIGNED) == 0;
  }

  public int getDisplaySize() {
    switch (dataType) {
      case VARCHAR:
      case JSON:
      case ENUM:
      case SET:
      case VARSTRING:
      case STRING:
        Integer maxWidth = CharsetEncodingLength.maxCharlen.get(charset);
        if (maxWidth == null) {
          return (int) length;
        }
        return (int) length / maxWidth;

      case DATE:
        return 10;
      case DATETIME:
      case TIMESTAMP:
        // S2 sends the same length of DATETIME(6) for both DATETIME(0) and DATETIME(6)
        // However in reality DATETIME(0) is 7 symbols shorter as it is missing ".000000"
        return (decimals == 0) ? (int) length - 7 : (int) length;
      case TIME:
        // same as above, but S2 returns 18 instead 17 for some reason
        return (decimals == 0) ? 10 : 17;

      case FLOAT:
        return 9;
      case DOUBLE:
        return 18;

      default:
        return (int) length;
    }
  }

  public boolean isPrimaryKey() {
    return (this.flags & ColumnFlags.PRIMARY_KEY) > 0;
  }

  public boolean isAutoIncrement() {
    return (this.flags & ColumnFlags.AUTO_INCREMENT) > 0;
  }

  public boolean hasDefault() {
    return (this.flags & ColumnFlags.NO_DEFAULT_VALUE_FLAG) == 0;
  }

  // doesn't use & 128 bit filter, because char binary and varchar binary are not binary (handle
  // like string), but have the binary flag
  public boolean isBinary() {
    return charset == 63;
  }

  public int getFlags() {
    return flags;
  }

  public String getExtTypeName() {
    return extTypeName;
  }

  /**
   * Return metadata precision.
   *
   * @return precision
   */
  public long getPrecision() {
    switch (dataType) {
      case OLDDECIMAL:
      case DECIMAL:
        // DECIMAL and OLDDECIMAL are  "exact" fixed-point number.
        // so :
        // - if can be signed, 1 byte is saved for sign
        // - if decimal > 0, one byte more for dot
        if (isSigned()) {
          return length - ((decimals > 0) ? 2 : 1);
        } else {
          return length - ((decimals > 0) ? 1 : 0);
        }
      case VARCHAR:
      case JSON:
      case ENUM:
      case SET:
      case VARSTRING:
      case STRING:
        Integer maxWidth = CharsetEncodingLength.maxCharlen.get(charset);
        if (maxWidth == null) {
          return length;
        }
        return length / maxWidth;
      case FLOAT:
        return 6;
      case DOUBLE:
        return 14;
      case DATE:
        return 10;
      case DATETIME:
      case TIMESTAMP:
        // S2 sends the same length of DATETIME(6) for both DATETIME(0) and DATETIME(6)
        // However in reality DATETIME(0) is 7 symbols shorter as it is missing ".000000"
        return (decimals == 0) ? length - 7 : length;
      case TIME:
        // same as above, but S2 returns 18 instead 17 for some reason
        return (decimals == 0) ? 10 : 17;

      default:
        return length;
    }
  }

  public int getColumnType(Configuration conf) {
    switch (dataType) {
      case TINYINT:
        // S2 always returns length 4 for TINYINT, so can't check it here
        if (conf.tinyInt1isBit()) {
          return Types.BIT;
        }
        return isSigned() ? Types.TINYINT : Types.SMALLINT;
      case BIT:
        if (length == 1) {
          return Types.BIT;
        }
        return Types.VARBINARY;
      case SMALLINT:
        return isSigned() ? Types.SMALLINT : Types.INTEGER;
      case INTEGER:
        return isSigned() ? Types.INTEGER : Types.BIGINT;
      case FLOAT:
        return Types.REAL;
      case DOUBLE:
        return Types.DOUBLE;
      case TIMESTAMP:
      case DATETIME:
        return Types.TIMESTAMP;
      case BIGINT:
        return Types.BIGINT;
      case MEDIUMINT:
        return Types.INTEGER;
      case DATE:
      case NEWDATE:
        return Types.DATE;
      case TIME:
        return Types.TIME;
      case YEAR:
        if (conf.yearIsDateType()) return Types.DATE;
        return Types.SMALLINT;
      case VARCHAR:
      case JSON:
      case ENUM:
      case SET:
      case VARSTRING:
      case TINYBLOB:
      case BLOB:
        return isBinary() ? Types.VARBINARY : Types.VARCHAR;
      case GEOMETRY:
        return Types.VARBINARY;
      case STRING:
        return isBinary() ? Types.VARBINARY : Types.CHAR;
      case OLDDECIMAL:
      case DECIMAL:
        return Types.DECIMAL;
      case MEDIUMBLOB:
      case LONGBLOB:
        return isBinary() ? Types.LONGVARBINARY : Types.LONGVARCHAR;
    }
    return Types.NULL;
  }

  public Codec<?> getDefaultCodec(Configuration conf) {
    switch (dataType) {
      case JSON:
      case VARCHAR:
      case ENUM:
      case SET:
      case VARSTRING:
      case STRING:
      case NULL:
        return StringCodec.INSTANCE;
      case TINYINT:
        return isSigned() ? ByteCodec.INSTANCE : ShortCodec.INSTANCE;
      case SMALLINT:
        return isSigned() ? ShortCodec.INSTANCE : IntCodec.INSTANCE;
      case INTEGER:
        return isSigned() ? IntCodec.INSTANCE : LongCodec.INSTANCE;
      case FLOAT:
        return FloatCodec.INSTANCE;
      case DOUBLE:
        return DoubleCodec.INSTANCE;
      case TIMESTAMP:
      case DATETIME:
        return TimestampCodec.INSTANCE;
      case BIGINT:
        return isSigned() ? LongCodec.INSTANCE : BigIntegerCodec.INSTANCE;
      case MEDIUMINT:
        return IntCodec.INSTANCE;
      case DATE:
      case NEWDATE:
        return DateCodec.INSTANCE;
      case OLDDECIMAL:
      case DECIMAL:
        return BigDecimalCodec.INSTANCE;
      case GEOMETRY:
        if (conf.geometryDefaultType() != null && "default".equals(conf.geometryDefaultType())) {
          switch (extTypeName) {
            case "point":
              return PointCodec.INSTANCE;
            case "linestring":
              return LineStringCodec.INSTANCE;
            case "polygon":
              return PolygonCodec.INSTANCE;
          }
        }
        return ByteArrayCodec.INSTANCE;
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case BLOB:
        return isBinary() ? BlobCodec.INSTANCE : ClobCodec.INSTANCE;
      case TIME:
        return TimeCodec.INSTANCE;
      case YEAR:
        if (conf.yearIsDateType()) return DateCodec.INSTANCE;
        return ShortCodec.INSTANCE;
      case BIT:
        return BitSetCodec.INSTANCE;
    }
    throw new IllegalArgumentException(String.format("Unexpected datatype %s", dataType));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnDefinitionPacket that = (ColumnDefinitionPacket) o;
    return charset == that.charset
        && length == that.length
        && dataType == that.dataType
        && decimals == that.decimals
        && flags == that.flags;
  }

  @Override
  public int hashCode() {
    return Objects.hash(charset, length, dataType, decimals, flags);
  }

  public void useAliasAsName() {
    useAliasAsName = true;
  }
}
