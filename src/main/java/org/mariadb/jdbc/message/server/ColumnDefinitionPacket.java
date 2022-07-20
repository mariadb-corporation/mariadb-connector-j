// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.server;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Locale;
import java.util.Objects;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.Column;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.message.ServerMessage;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.plugin.codec.*;
import org.mariadb.jdbc.util.CharsetEncodingLength;
import org.mariadb.jdbc.util.constants.ColumnFlags;

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
   */
  private ColumnDefinitionPacket(
      ReadableByteBuf buf, long length, DataType dataType, int[] stringPos) {
    this.buf = buf;
    this.charset = 33;
    this.length = length;
    this.dataType = dataType;
    this.decimals = (byte) 0;
    this.flags = ColumnFlags.AUTO_INCREMENT | ColumnFlags.UNSIGNED;
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
   * @return column definition
   */
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
        new StandardReadableByteBuf(arr, arr.length), len, type, stringPos);
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

  public byte getDecimals() {
    return decimals;
  }

  public boolean isSigned() {
    return (flags & ColumnFlags.UNSIGNED) == 0;
  }

  public int getDisplaySize() {
    if (!isBinary()
        && (dataType == DataType.VARCHAR
            || dataType == DataType.JSON
            || dataType == DataType.ENUM
            || dataType == DataType.SET
            || dataType == DataType.VARSTRING
            || dataType == DataType.STRING
            || dataType == DataType.BLOB
            || dataType == DataType.TINYBLOB
            || dataType == DataType.MEDIUMBLOB
            || dataType == DataType.LONGBLOB)) {
      Integer maxWidth = CharsetEncodingLength.maxCharlen.get(charset);
      if (maxWidth != null) return (int) (length / maxWidth);
    }
    return (int) length;
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
  public int getPrecision() {
    switch (dataType) {
      case OLDDECIMAL:
      case DECIMAL:
        // DECIMAL and OLDDECIMAL are  "exact" fixed-point number.
        // so :
        // - if is signed, 1 byte is saved for sign
        // - if decimal > 0, one byte more for dot
        if (isSigned()) {
          return (int) (length - ((decimals > 0) ? 2 : 1));
        } else {
          return (int) (length - ((decimals > 0) ? 1 : 0));
        }
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
        return (int) (length / maxWidth);

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (!isBinary()) {
          Integer maxWidth2 = CharsetEncodingLength.maxCharlen.get(charset);
          if (maxWidth2 != null) return (int) (length / maxWidth2);
        }
        return (int) length;

      default:
        return (int) length;
    }
  }

  public String getColumnTypeName(Configuration conf) {
    switch (dataType) {
      case TINYINT:
        if (length == 1) {
          return conf.transformedBitIsBoolean() ? "BOOLEAN" : "BIT";
        }
        if (!isSigned()) {
          return dataType.name() + " UNSIGNED";
        } else {
          return dataType.name();
        }
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
        if (!isSigned()) {
          return dataType.name() + " UNSIGNED";
        } else {
          return dataType.name();
        }
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        /*
         map to different blob types based on datatype length
         see https://mariadb.com/kb/en/library/data-types/
        */
        if (extTypeFormat != null) {
          return extTypeFormat.toUpperCase(Locale.ROOT);
        }
        if (isBinary()) {
          if (length < 0) {
            return "LONGBLOB";
          } else if (length <= 255) {
            return "TINYBLOB";
          } else if (length <= 65535) {
            return "BLOB";
          } else if (length <= 16777215) {
            return "MEDIUMBLOB";
          } else {
            return "LONGBLOB";
          }
        } else {
          if (length < 0) {
            return "LONGTEXT";
          } else if (getDisplaySize() <= 65532) {
            return "VARCHAR";
          } else if (getDisplaySize() <= 65535) {
            return "TEXT";
          } else if (getDisplaySize() <= 16777215) {
            return "MEDIUMTEXT";
          } else {
            return "LONGTEXT";
          }
        }
      case VARSTRING:
      case VARCHAR:
        if (isBinary()) {
          return "VARBINARY";
        }
        if (length < 0) {
          return "LONGTEXT";
        } else if (getDisplaySize() <= 65532) {
          return "VARCHAR";
        } else if (getDisplaySize() <= 65535) {
          return "TEXT";
        } else if (getDisplaySize() <= 16777215) {
          return "MEDIUMTEXT";
        } else {
          return "LONGTEXT";
        }
      case STRING:
        if (isBinary()) {
          return "BINARY";
        }
        return "CHAR";
      case GEOMETRY:
        if (extTypeName != null) {
          return extTypeName.toUpperCase(Locale.ROOT);
        }
        return dataType.name();
      default:
        return dataType.name();
    }
  }

  public int getColumnType(Configuration conf) {
    switch (dataType) {
      case TINYINT:
        if (length == 1) {
          return conf.transformedBitIsBoolean() ? Types.BOOLEAN : Types.BIT;
        }
        return isSigned() ? Types.TINYINT : Types.SMALLINT;
      case BIT:
        if (length == 1) {
          return Types.BOOLEAN;
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
      case JSON:
        return Types.LONGVARCHAR;
      case VARCHAR:
      case ENUM:
      case SET:
      case VARSTRING:
      case TINYBLOB:
      case BLOB:
        if (length <= 0 || getDisplaySize() > 16777215) {
          return isBinary() ? Types.LONGVARBINARY : Types.LONGVARCHAR;
        } else {
          return isBinary() ? Types.VARBINARY : Types.VARCHAR;
        }
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
        return StringCodec.INSTANCE;
      case NULL:
      case SET:
      case ENUM:
      case VARCHAR:
      case VARSTRING:
      case STRING:
        return isBinary() ? ByteArrayCodec.INSTANCE : StringCodec.INSTANCE;
      case TINYINT:
        if (conf.tinyInt1isBit() && this.length == 1) return BooleanCodec.INSTANCE;
        return IntCodec.INSTANCE;
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
          if (extTypeName != null) {
            switch (extTypeName) {
              case "point":
                return PointCodec.INSTANCE;
              case "linestring":
                return LineStringCodec.INSTANCE;
              case "polygon":
                return PolygonCodec.INSTANCE;
              case "multipoint":
                return MultiPointCodec.INSTANCE;
              case "multilinestring":
                return MultiLinestringCodec.INSTANCE;
              case "multipolygon":
                return MultiPolygonCodec.INSTANCE;
              case "geometrycollection":
                return GeometryCollectionCodec.INSTANCE;
            }
          }
          return GeometryCollectionCodec.INSTANCE;
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
      case BIT:
        if (this.length == 1) return BooleanCodec.INSTANCE;
        return ByteArrayCodec.INSTANCE;
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
