// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.column;

import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Types;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;

/** Column metadata definition */
public class YearColumn extends SmallIntColumn {

  public YearColumn(
      ReadableByteBuf buf,
      int charset,
      long length,
      DataType dataType,
      byte decimals,
      int flags,
      int[] stringPos,
      String extTypeName,
      String extTypeFormat) {
    super(buf, charset, length, dataType, decimals, flags, stringPos, extTypeName, extTypeFormat);
  }

  public String defaultClassname(Configuration conf) {
    return conf.yearIsDateType() ? Date.class.getName() : Short.class.getName();
  }

  public int getColumnType(Configuration conf) {
    return conf.yearIsDateType() ? Types.DATE : Types.SMALLINT;
  }

  public String getColumnTypeName(Configuration conf) {
    return "YEAR";
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, int length)
      throws SQLDataException {
    if (conf.yearIsDateType()) {
      short y = (short) buf.atoull(length);
      if (columnLength == 2) {
        // YEAR(2) - deprecated
        if (y <= 69) {
          y += 2000;
        } else {
          y += 1900;
        }
      }
      return Date.valueOf(y + "-01-01");
    }
    return decodeShortText(buf, length);
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, int length)
      throws SQLDataException {
    if (conf.yearIsDateType()) {
      int v = buf.readShort();
      if (columnLength == 2) {
        // YEAR(2) - deprecated
        if (v <= 69) {
          v += 2000;
        } else {
          v += 1900;
        }
      }
      return Date.valueOf(v + "-01-01");
    }
    return decodeShortText(buf, length);
  }
}
