// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.column;

import java.sql.*;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;

/** Column metadata definition */
public class YearColumn extends UnsignedSmallIntColumn {

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

  @Override
  public Date decodeDateText(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    short y = (short) buf.atoll(length);
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

  @Override
  public Date decodeDateBinary(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
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

  @Override
  public Timestamp decodeTimestampText(ReadableByteBuf buf, int length, Calendar calParam)
      throws SQLDataException {
    Calendar cal1 = calParam == null ? Calendar.getInstance() : calParam;

    int year = Integer.parseInt(buf.readAscii(length));
    if (columnLength <= 2) year += year >= 70 ? 1900 : 2000;
    synchronized (cal1) {
      cal1.clear();
      cal1.set(year, Calendar.JANUARY, 1);
      return new Timestamp(cal1.getTimeInMillis());
    }
  }

  @Override
  public Timestamp decodeTimestampBinary(ReadableByteBuf buf, int length, Calendar calParam)
      throws SQLDataException {
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;

    int year = buf.readUnsignedShort();
    if (columnLength <= 2) year += year >= 70 ? 1900 : 2000;

    Timestamp timestamp;
    synchronized (cal) {
      cal.clear();
      cal.set(year, 0, 1, 0, 0, 0);
      timestamp = new Timestamp(cal.getTimeInMillis());
    }
    timestamp.setNanos(0);
    return timestamp;
  }
}
