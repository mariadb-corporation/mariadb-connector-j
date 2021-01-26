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

package org.mariadb.jdbc.message.server;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Objects;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.codec.list.*;
import org.mariadb.jdbc.util.CharsetEncodingLength;
import org.mariadb.jdbc.util.constants.ColumnFlags;

public class ColumnDefinitionPacket implements ServerMessage {

  private final ReadableByteBuf buf;
  private final int charset;
  private final long length;
  private final DataType dataType;
  private final byte decimals;
  private final int flags;
  private boolean useAliasAsName;

  private ColumnDefinitionPacket(
      ReadableByteBuf buf, int charset, long length, DataType dataType, byte decimals, int flags) {
    this.buf = buf;
    this.charset = charset;
    this.length = length;
    this.dataType = dataType;
    this.decimals = decimals;
    this.flags = flags;
  }

  public ColumnDefinitionPacket(ReadableByteBuf buf) {
    // read from end, avoiding to parse string meta that are usually not needed.
    buf.pos(buf.readableBytes() - 12);
    this.buf = buf;
    this.charset = buf.readShort();
    this.length = buf.readInt();
    this.dataType = DataType.of(buf.readUnsignedByte());
    this.flags = buf.readUnsignedShort();
    this.decimals = buf.readByte();
  }

  public ColumnDefinitionPacket(ColumnDefinitionPacket col) {
    this.buf = col.buf;
    this.charset = col.charset;
    this.length = col.length;
    this.dataType = col.dataType;
    this.decimals = col.decimals;
    this.flags = col.flags;
  }

  public static ColumnDefinitionPacket create(String name, DataType type) {
    byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    byte[] arr = new byte[9 + 2 * nameBytes.length];
    arr[0] = 3;
    arr[1] = 'D';
    arr[2] = 'E';
    arr[3] = 'F';

    // lenenc_str     name
    // lenenc_str     org_name
    int pos = 7;
    for (int i = 0; i < 2; i++) {
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
        ColumnFlags.PRIMARY_KEY);
  }

  private String getString(int idx) {
    buf.pos(4);
    for (int i = 0; i < idx; i++) {
      buf.skip(buf.readLengthNotNull());
    }
    return buf.readString(buf.readLengthNotNull());
  }

  public String getSchema() {
    return getString(0);
  }

  public String getTableAlias() {
    return getString(1);
  }

  public String getTable() {
    return getString(useAliasAsName ? 1 : 2);
  }

  public String getColumnAlias() {
    return getString(3);
  }

  public String getColumn() {
    return getString(4);
  }

  public int getCharset() {
    return charset;
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
    if (dataType == DataType.VARCHAR
        || dataType == DataType.JSON
        || dataType == DataType.ENUM
        || dataType == DataType.SET
        || dataType == DataType.VARSTRING
        || dataType == DataType.STRING) {
      Integer maxWidth = CharsetEncodingLength.maxCharlen.get(charset);
      if (maxWidth == null) {
        return (int) length;
      }
      return (int) length / maxWidth;
    }
    return (int) length;
  }

  public boolean getNullability() {
    return (flags & ColumnFlags.NOT_NULL) == 0;
  }

  public boolean isPrimaryKey() {
    return (this.flags & ColumnFlags.PRIMARY_KEY) > 0;
  }

  public boolean isUniqueKey() {
    return (this.flags & ColumnFlags.UNIQUE_KEY) > 0;
  }

  public boolean isMultipleKey() {
    return (this.flags & ColumnFlags.MULTIPLE_KEY) > 0;
  }

  public boolean isBlob() {
    return (this.flags & ColumnFlags.BLOB) > 0;
  }

  public boolean isZeroFill() {
    return (this.flags & ColumnFlags.ZEROFILL) > 0;
  }

  public boolean isNullable() {
    return !((this.flags & ColumnFlags.NOT_NULL) > 0);
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
      default:
        return length;
    }
  }

  public int getColumnType(Configuration conf) {
    switch (dataType) {
      case TINYINT:
        if (length == 1) {
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
      case GEOMETRY:
        return isBinary() ? Types.VARBINARY : Types.VARCHAR;
      case STRING:
        return isBinary() ? Types.BINARY : Types.CHAR;
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
      case VARCHAR:
      case JSON:
      case ENUM:
      case SET:
      case VARSTRING:
      case STRING:
        return isBinary() ? ByteArrayCodec.INSTANCE : StringCodec.INSTANCE;
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

  @Override
  public String toString() {
    return "ColumnDefinitionPacket{"
        + "charset="
        + charset
        + ", length="
        + length
        + ", dataType="
        + dataType
        + ", decimals="
        + decimals
        + ", flags="
        + flags
        + '}';
  }

  public void useAliasAsName() {
    useAliasAsName = true;
  }
}
