// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.server;

import java.util.Objects;
import org.mariadb.jdbc.client.Column;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.ServerMessage;
import org.mariadb.jdbc.util.CharsetEncodingLength;
import org.mariadb.jdbc.util.constants.ColumnFlags;

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

  public long getColumnLength() {
    return columnLength;
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
      if (maxWidth != null) return (int) (columnLength / maxWidth);
    }
    return (int) columnLength;
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

  public void useAliasAsName() {
    useAliasAsName = true;
  }
}
