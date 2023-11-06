// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import java.sql.*;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;

/** Column metadata definition */
public class YearColumn extends UnsignedSmallIntColumn {

  /**
   * YEAR metadata type decoder
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
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    if (conf.yearIsDateType()) {
      short y = (short) buf.atoull(length.get());
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
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, MutableInt length)
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
  public Date decodeDateText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    short y = (short) buf.atoll(length.get());
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
  public Date decodeDateBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
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
  public Timestamp decodeTimestampText(ReadableByteBuf buf, MutableInt length, Calendar calParam)
      throws SQLDataException {
    int year = Integer.parseInt(buf.readAscii(length.get()));
    if (columnLength <= 2) year += year >= 70 ? 1900 : 2000;

    if (calParam == null) {
      Calendar cal1 = Calendar.getInstance();
      cal1.clear();
      cal1.set(year, Calendar.JANUARY, 1);
      return new Timestamp(cal1.getTimeInMillis());
    } else {
      synchronized (calParam) {
        calParam.clear();
        calParam.set(year, Calendar.JANUARY, 1);
        return new Timestamp(calParam.getTimeInMillis());
      }
    }
  }

  @Override
  public Timestamp decodeTimestampBinary(ReadableByteBuf buf, MutableInt length, Calendar calParam)
      throws SQLDataException {
    int year = buf.readUnsignedShort();
    if (columnLength <= 2) year += year >= 70 ? 1900 : 2000;

    Timestamp timestamp;
    if (calParam == null) {
      Calendar cal = Calendar.getInstance();
      cal.clear();
      cal.set(year, 0, 1, 0, 0, 0);
      timestamp = new Timestamp(cal.getTimeInMillis());
    } else {
      synchronized (calParam) {
        calParam.clear();
        calParam.set(year, 0, 1, 0, 0, 0);
        timestamp = new Timestamp(calParam.getTimeInMillis());
      }
    }
    timestamp.setNanos(0);
    return timestamp;
  }
}
