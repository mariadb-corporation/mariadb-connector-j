// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import java.sql.*;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.util.CharsetEncodingLength;

/** Column metadata definition */
public class JsonColumn extends StringColumn implements ColumnDecoder {

  /**
   * JSON metadata type decoder
   *
   * @param buf buffer
   * @param charset charset
   * @param length maximum data length
   * @param dataType data type. see https://mariadb.com/kb/en/result-set-packets/#field-types
   * @param decimals decimal length
   * @param flags flags. see https://mariadb.com/kb/en/result-set-packets/#field-details-flag
   * @param stringPos string offset position in buffer
   * @param extTypeName extended type name
   * @param extTypeFormat extended type format
   */
  public JsonColumn(
      final ReadableByteBuf buf,
      final int charset,
      final long length,
      final DataType dataType,
      final byte decimals,
      final int flags,
      final int[] stringPos,
      final String extTypeName,
      final String extTypeFormat) {
    super(buf, charset, length, dataType, decimals, flags, stringPos, extTypeName, extTypeFormat);
  }

  /**
   * Recreate new column using alias as name.
   *
   * @param prev current column
   */
  protected JsonColumn(JsonColumn prev) {
    super(prev);
  }

  public int getDisplaySize() {
    if (charset != 63) {
      Integer maxWidth = CharsetEncodingLength.maxCharlen.get(charset);
      if (maxWidth != null) return (int) (columnLength / maxWidth);
      return (int) (columnLength / 4);
    }
    return (int) columnLength;
  }

  @Override
  public JsonColumn useAliasAsName() {
    return new JsonColumn(this);
  }

  public String defaultClassname(final Configuration conf) {
    return String.class.getName();
  }

  public int getColumnType(final Configuration conf) {
    return Types.LONGVARCHAR;
  }

  public String getColumnTypeName(final Configuration conf) {
    return "JSON";
  }
}
