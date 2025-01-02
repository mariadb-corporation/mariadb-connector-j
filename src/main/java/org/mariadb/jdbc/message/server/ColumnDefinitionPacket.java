// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.message.server;

import java.util.Objects;
import org.mariadb.jdbc.client.Column;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.ServerMessage;
import org.mariadb.jdbc.util.constants.ColumnFlags;

/** Column metadata definition */
public class ColumnDefinitionPacket implements Column, ServerMessage {

  /** charset */
  protected final int charset;

  /** column maximum length */
  protected final long columnLength;

  /**
   * data type @see <a href="https://mariadb.com/kb/en/result-set-packets/#field-types">Field
   * type</a>
   */
  protected final DataType dataType;

  /** number of decimal */
  protected final byte decimals;

  /** extended type name */
  protected final String extTypeName;

  /** extended type format */
  protected final String extTypeFormat;

  private final ReadableByteBuf buf;

  /**
   * @see <a href="https://mariadb.com/kb/en/result-set-packets/#field-details-flag">flags</a>
   */
  private final int flags;

  /** string offset position in buffer */
  private final int[] stringPos;

  /** configuration: use alias as name */
  private final boolean useAliasAsName;

  /**
   * Column definition constructor
   *
   * @param buf buffer
   * @param charset charset
   * @param columnLength maxium column length
   * @param dataType data type
   * @param decimals decimal length
   * @param flags flags
   * @param stringPos string position indexes
   * @param extTypeName extended type name
   * @param extTypeFormat extended type format
   * @param useAliasAsName use alias as name
   */
  public ColumnDefinitionPacket(
      ReadableByteBuf buf,
      int charset,
      long columnLength,
      DataType dataType,
      byte decimals,
      int flags,
      int[] stringPos,
      String extTypeName,
      String extTypeFormat,
      boolean useAliasAsName) {
    this.buf = buf;
    this.charset = charset;
    this.columnLength = columnLength;
    this.dataType = dataType;
    this.decimals = decimals;
    this.flags = flags;
    this.stringPos = stringPos;
    this.extTypeName = extTypeName;
    this.extTypeFormat = extTypeFormat;
    this.useAliasAsName = useAliasAsName;
  }

  protected ColumnDefinitionPacket(ColumnDefinitionPacket prev, boolean useAliasAsName) {
    this.buf = prev.buf;
    this.charset = prev.charset;
    this.columnLength = prev.columnLength;
    this.dataType = prev.dataType;
    this.decimals = prev.decimals;
    this.flags = prev.flags;
    this.stringPos = prev.stringPos;
    this.extTypeName = prev.extTypeName;
    this.extTypeFormat = prev.extTypeFormat;
    this.useAliasAsName = useAliasAsName;
  }

  public String getCatalog() {
    return "def";
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
}
