// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.server;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.client.Column;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.impl.StandardReadableByteBuf;
import com.singlestore.jdbc.message.ServerMessage;
import com.singlestore.jdbc.plugin.Codec;
import com.singlestore.jdbc.plugin.codec.BigDecimalCodec;
import com.singlestore.jdbc.plugin.codec.BigIntegerCodec;
import com.singlestore.jdbc.plugin.codec.BitSetCodec;
import com.singlestore.jdbc.plugin.codec.BlobCodec;
import com.singlestore.jdbc.plugin.codec.BooleanCodec;
import com.singlestore.jdbc.plugin.codec.ByteArrayCodec;
import com.singlestore.jdbc.plugin.codec.ByteCodec;
import com.singlestore.jdbc.plugin.codec.DateCodec;
import com.singlestore.jdbc.plugin.codec.DoubleCodec;
import com.singlestore.jdbc.plugin.codec.FloatCodec;
import com.singlestore.jdbc.plugin.codec.IntCodec;
import com.singlestore.jdbc.plugin.codec.LineStringCodec;
import com.singlestore.jdbc.plugin.codec.LongCodec;
import com.singlestore.jdbc.plugin.codec.PointCodec;
import com.singlestore.jdbc.plugin.codec.PolygonCodec;
import com.singlestore.jdbc.plugin.codec.ShortCodec;
import com.singlestore.jdbc.plugin.codec.StringCodec;
import com.singlestore.jdbc.plugin.codec.TimeCodec;
import com.singlestore.jdbc.plugin.codec.TimestampCodec;
import com.singlestore.jdbc.util.CharsetEncodingLength;
import com.singlestore.jdbc.util.constants.ColumnFlags;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Objects;

/** Column metadata definition */
public class ColumnDefinitionPacket implements Column, ServerMessage {

  private final ReadableByteBuf buf;
  private final int charset;
  private final long length;
  private final DataType dataType;
  private final byte decimals;
  private final int flags;
  private final int[] stringPos;
  private final String extTypeName;
  private final String extTypeFormat;
  private boolean useAliasAsName;

  /**
   * constructor for generated metadata
   *
   * @param buf buffer
   * @param length length
   * @param dataType server data type
   * @param stringPos string information position
   * @param flags columns flags
   */
  private ColumnDefinitionPacket(
      ReadableByteBuf buf, long length, DataType dataType, int[] stringPos, int flags) {
    this.buf = buf;
    this.charset = 33;
    this.length = length;
    this.dataType = dataType;
    this.decimals = (byte) 0;
    this.flags = flags;
    this.stringPos = stringPos;
    this.extTypeName = null;
    this.extTypeFormat = null;
  }

  /**
   * Generate object from mysql packet
   *
   * @param buf mysql packet buffer
   * @param extendedInfo support extended information
   */
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
      String tmpTypeFormat = null;

      // fast skipping extended info (usually not set)
      if (buf.readByte() != 0) {
        // revert position, because has extended info.
        buf.pos(buf.pos() - 1);

        ReadableByteBuf subPacket = buf.readLengthBuffer();
        while (subPacket.readableBytes() > 0) {
          switch (subPacket.readByte()) {
            case 0:
              tmpTypeName = subPacket.readAscii(subPacket.readLength());
              break;
            case 1:
              tmpTypeFormat = subPacket.readAscii(subPacket.readLength());
              break;
            default: // skip data
              subPacket.skip(subPacket.readLength());
              break;
          }
        }
      }
      extTypeName = tmpTypeName;
      extTypeFormat = tmpTypeFormat;
    } else {
      extTypeName = null;
      extTypeFormat = null;
    }

    this.buf = buf;
    buf.skip(); // skip length always 0x0c
    this.charset = buf.readShort();
    this.length = buf.readInt();
    this.dataType = DataType.of(buf.readUnsignedByte());
    this.flags = buf.readUnsignedShort();
    this.decimals = buf.readByte();
  }

  /**
   * Generate column definition from name
   *
   * @param name column name
   * @param type server type
   * @param flags columns flags
   * @return column definition
   */
  public static ColumnDefinitionPacket create(String name, DataType type, int flags) {
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
        new StandardReadableByteBuf(arr, arr.length), len, type, stringPos, flags);
  }

  public String getSchema() {
    buf.pos(stringPos[0]);
    return buf.readString(buf.readIntLengthEncodedNotNull());
  }

  public String getTableAlias() {
    buf.pos(stringPos[1]);
    return buf.readString(buf.readIntLengthEncodedNotNull());
  }

  public String getTable() {
    buf.pos(stringPos[useAliasAsName ? 1 : 2]);
    return buf.readString(buf.readIntLengthEncodedNotNull());
  }

  public String getColumnAlias() {
    buf.pos(stringPos[3]);
    return buf.readString(buf.readIntLengthEncodedNotNull());
  }

  public String getColumnName() {
    buf.pos(stringPos[4]);
    return buf.readString(buf.readIntLengthEncodedNotNull());
  }

  public long getLength() {
    return length;
  }

  public DataType getType() {
    return dataType;
  }

  public String getTypeName(Configuration conf) {
    switch (dataType) {
      case TINYINT:
        if (conf.tinyInt1isBit()) {
          return conf.transformedBitIsBoolean() ? "BOOLEAN" : "BIT";
        }
        if (!isSigned()) {
          return dataType.name() + " UNSIGNED";
        } else {
          return dataType.name();
        }
      case SMALLINT:
      case MEDIUMINT:
      case INT:
      case DOUBLE:
      case BIGINT:
        if (!isSigned()) {
          return dataType.name() + " UNSIGNED";
        } else {
          return dataType.name();
        }
      case VARCHAR:
        return isBinary() ? "VARBINARY" : "VARCHAR";
      case CHAR:
        return isBinary() ? "BINARY" : "CHAR";
      case TINYBLOB:
        return isBinary() ? "TINYBLOB" : "TINYTEXT";
      case BLOB:
        return isBinary() ? "BLOB" : "TEXT";
      case MEDIUMBLOB:
        return isBinary() ? "MEDIUMBLOB" : "MEDIUMTEXT";
      case LONGBLOB:
        return isBinary() ? "LONGBLOB" : "LONGTEXT";
      default:
        return dataType.name();
    }
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

  @Override
  public int getDisplaySize() {
    switch (dataType) {
      case VARCHAR:
      case JSON:
      case ENUM:
      case SET:
      case CHAR:
      case MEDIUMBLOB:
      case BLOB:
      case TINYBLOB:
        Integer maxWidth = CharsetEncodingLength.maxCharlen.get(charset);
        if (maxWidth == null) {
          return (int) length;
        }
        return (int) length / maxWidth;

        // server sends MAX_UNSIGNEDINT or 4GB (or -1 if interpreted as int) as length for this.
        // For LONGBLOB with maxWidth 1 this doesn't fit into an int, so return MAXINT.
        // For LONGTEXT with maxWidth of at least 2, the precision fits
      case LONGBLOB:
        maxWidth = CharsetEncodingLength.maxCharlen.get(charset);
        if (maxWidth == null) {
          return Integer.MAX_VALUE;
        }
        return (int) Long.max(Long.divideUnsigned(length, maxWidth.longValue()), Integer.MAX_VALUE);

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
        return 12;
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

  @Override
  public int getFlags() {
    return flags;
  }

  @Override
  public String getExtTypeName() {
    return extTypeName;
  }

  /**
   * Return metadata precision.
   *
   * @return precision
   */
  @Override
  public int getPrecision() {
    switch (dataType) {
      case OLDDECIMAL:
      case DECIMAL:
        // DECIMAL and OLDDECIMAL are  "exact" fixed-point number.
        // so :
        // - if can be signed, 1 byte is saved for sign
        // - if decimal > 0, one byte more for dot
        if (isSigned()) {
          return (int) length - ((decimals > 0) ? 2 : 1);
        } else {
          return (int) length - ((decimals > 0) ? 1 : 0);
        }

      case FLOAT:
        return 12;
      case DOUBLE:
        return 22;
      case INT:
        return 10;
      case TINYINT:
        return 3;
      case SMALLINT:
        return 5;
      case MEDIUMINT:
        return 7;
      case BIGINT:
        return 19;

      case VARCHAR:
      case JSON:
      case ENUM:
      case SET:
      case CHAR:
      case DATE:
      case DATETIME:
      case TIMESTAMP:
      case TIME:
      case LONGBLOB:
      case MEDIUMBLOB:
      case BLOB:
      case TINYBLOB:
        // Character types, precision should equal number of displayed characters
        return getDisplaySize();

      default:
        return (int) Math.max(length, 0);
    }
  }

  @Override
  public int getColumnType(Configuration conf) {
    switch (dataType) {
      case TINYINT:
        // S2 always returns length 4 for TINYINT, so can't check it here
        if (conf.tinyInt1isBit()) {
          return conf.transformedBitIsBoolean() ? Types.BOOLEAN : Types.BIT;
        }
        return isSigned() ? Types.TINYINT : Types.SMALLINT;
      case BIT:
        if (length == 1) {
          return Types.BIT;
        }
        return Types.VARBINARY;
      case SMALLINT:
        return isSigned() ? Types.SMALLINT : Types.INTEGER;
      case INT:
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
      case ENUM:
      case SET:
      case TINYBLOB:
        return isBinary() ? Types.VARBINARY : Types.VARCHAR;
      case GEOMETRY:
        return Types.VARBINARY;
      case CHAR:
        return isBinary() ? Types.BINARY : Types.CHAR;
      case OLDDECIMAL:
      case DECIMAL:
        return Types.DECIMAL;
      case MEDIUMBLOB:
      case LONGBLOB:
      case BLOB:
      case JSON:
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
      case CHAR:
      case NULL:
        return StringCodec.INSTANCE;
      case TINYINT:
        if (conf.tinyInt1isBit() && conf.transformedBitIsBoolean()) {
          return BooleanCodec.INSTANCE;
        }
        return isSigned() ? ByteCodec.INSTANCE : ShortCodec.INSTANCE;
      case BIT:
        return BitSetCodec.INSTANCE;
      case SMALLINT:
        return isSigned() ? ShortCodec.INSTANCE : IntCodec.INSTANCE;
      case INT:
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
        return isBinary() ? BlobCodec.INSTANCE : StringCodec.INSTANCE;
      case TIME:
        return TimeCodec.INSTANCE;
      case YEAR:
        if (conf.yearIsDateType()) return DateCodec.INSTANCE;
        return ShortCodec.INSTANCE;
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
