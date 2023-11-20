// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.server;

import com.singlestore.jdbc.client.Column;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.message.ServerMessage;
import com.singlestore.jdbc.util.CharsetEncodingLength;
import com.singlestore.jdbc.util.constants.ColumnFlags;
import java.util.Objects;

/** Column metadata definition */
public class ColumnDefinitionPacket implements Column, ServerMessage {

  private final ReadableByteBuf buf;
  protected final int charset;
  protected final long columnLength;
  protected final DataType dataType;
  protected final byte decimals;
  private final int flags;
  private final int[] stringPos;
  protected final String extTypeName;
  protected final String extTypeFormat;
  private boolean useAliasAsName;

  public ColumnDefinitionPacket(
      ReadableByteBuf buf,
      int charset,
      long columnLength,
      DataType dataType,
      byte decimals,
      int flags,
      int[] stringPos,
      String extTypeName,
      String extTypeFormat) {
    this.buf = buf;
    this.charset = charset;
    this.columnLength = columnLength;
    this.dataType = dataType;
    this.decimals = decimals;
    this.flags = flags;
    this.stringPos = stringPos;
    this.extTypeName = extTypeName;
    this.extTypeFormat = extTypeFormat;
  }

  @Override
  public String getSchema() {
    buf.pos(stringPos[0]);
    return buf.readString(buf.readIntLengthEncodedNotNull());
  }

  @Override
  public String getTableAlias() {
    buf.pos(stringPos[1]);
    return buf.readString(buf.readIntLengthEncodedNotNull());
  }

  @Override
  public String getTable() {
    buf.pos(stringPos[useAliasAsName ? 1 : 2]);
    return buf.readString(buf.readIntLengthEncodedNotNull());
  }

  @Override
  public String getColumnAlias() {
    buf.pos(stringPos[3]);
    return buf.readString(buf.readIntLengthEncodedNotNull());
  }

  @Override
  public String getColumnName() {
    buf.pos(stringPos[4]);
    return buf.readString(buf.readIntLengthEncodedNotNull());
  }

  @Override
  public long getColumnLength() {
    return columnLength;
  }

  @Override
  public DataType getType() {
    return dataType;
  }

  @Override
  public byte getDecimals() {
    if (dataType == DataType.DATE) {
      return 0;
    }
    return decimals;
  }

  @Override
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
          return (int) columnLength;
        }
        return (int) columnLength / maxWidth;

        // server sends MAX_UNSIGNEDINT or 4GB (or -1 if interpreted as int) as length for this.
        // For LONGBLOB with maxWidth 1 this doesn't fit into an int, so return MAXINT.
        // For LONGTEXT with maxWidth of at least 2, the precision fits
      case LONGBLOB:
        maxWidth = CharsetEncodingLength.maxCharlen.get(charset);
        if (maxWidth == null) {
          return Integer.MAX_VALUE;
        }
        return (int)
            Long.max(Long.divideUnsigned(columnLength, maxWidth.longValue()), Integer.MAX_VALUE);

      case DATE:
        return 10;
      case DATETIME:
      case TIMESTAMP:
        // S2 sends the same length of DATETIME(6) for both DATETIME(0) and DATETIME(6)
        // However in reality DATETIME(0) is 7 symbols shorter as it is missing ".000000"
        return (decimals == 0) ? (int) columnLength - 7 : (int) columnLength;
      case TIME:
        // same as above, but S2 returns 18 instead 17 for some reason
        return (decimals == 0) ? 10 : 17;

      case FLOAT:
        return 12;
      case DOUBLE:
        return 18;

      default:
        return (int) columnLength;
    }
  }

  @Override
  public boolean isPrimaryKey() {
    return (this.flags & ColumnFlags.PRIMARY_KEY) > 0;
  }

  @Override
  public boolean isAutoIncrement() {
    return (this.flags & ColumnFlags.AUTO_INCREMENT) > 0;
  }

  @Override
  public boolean hasDefault() {
    return (this.flags & ColumnFlags.NO_DEFAULT_VALUE_FLAG) == 0;
  }

  // doesn't use & 128 bit filter, because char binary and varchar binary are not binary (handle
  // like string), but have the binary flag
  @Override
  public boolean isBinary() {
    return charset == 63;
  }

  @Override
  public int getFlags() {
    return flags;
  }

  public String getExtTypeName() {
    return extTypeName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnDefinitionPacket that = (ColumnDefinitionPacket) o;
    return charset == that.charset
        && columnLength == that.columnLength
        && dataType == that.dataType
        && decimals == that.decimals
        && flags == that.flags;
  }

  @Override
  public int hashCode() {
    return Objects.hash(charset, columnLength, dataType, decimals, flags);
  }

  @Override
  public void useAliasAsName() {
    useAliasAsName = true;
  }
}
