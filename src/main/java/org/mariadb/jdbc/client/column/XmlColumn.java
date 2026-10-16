// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.client.column;

import java.sql.SQLDataException;
import java.sql.SQLXML;
import java.sql.Types;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.MariaDbSqlXml;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;

/**
 * XMLTYPE column (MariaDB 12.3+): sent as a text LONGBLOB with the extended type name "xml".
 * Behaves as a text column for all getters, and as a {@link SQLXML} for {@code getObject}.
 */
public class XmlColumn extends BlobColumn implements ColumnDecoder {

  public XmlColumn(
      final ReadableByteBuf buf,
      final int charset,
      final long length,
      final DataType dataType,
      final byte decimals,
      final int flags,
      final int[] stringPos,
      final byte[] extTypeName,
      final byte[] extTypeFormat) {
    super(buf, charset, length, dataType, decimals, flags, stringPos, extTypeName, extTypeFormat);
  }

  protected XmlColumn(XmlColumn prev) {
    super(prev);
  }

  @Override
  public XmlColumn useAliasAsName() {
    return new XmlColumn(this);
  }

  @Override
  public String defaultClassname(final Configuration conf) {
    return SQLXML.class.getName();
  }

  @Override
  public int getColumnType(final Configuration conf) {
    return Types.SQLXML;
  }

  @Override
  public String getColumnTypeName(final Configuration conf) {
    return "XML";
  }

  @Override
  public Object getDefaultText(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException {
    return new MariaDbSqlXml(buf.readString(length.get()));
  }

  @Override
  public Object getDefaultBinary(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException {
    return getDefaultText(buf, length, context);
  }
}
