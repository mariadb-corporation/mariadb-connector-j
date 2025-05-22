// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import java.io.IOException;
import java.sql.*;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.impl.readable.BufferedReadableByteBuf;
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
      final BufferedReadableByteBuf buf,
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
  protected YearColumn(YearColumn prev) {
    super(prev);
  }

  @Override
  public YearColumn useAliasAsName() {
    return new YearColumn(this);
  }

  public String defaultClassname(final Configuration conf) {
    return conf.yearIsDateType() ? Date.class.getName() : Short.class.getName();
  }

  public int getColumnType(final Configuration conf) {
    return conf.yearIsDateType() ? Types.DATE : Types.SMALLINT;
  }

  public String getColumnTypeName(final Configuration conf) {
    return "YEAR";
  }

  @Override
  public Object getDefaultText(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException, IOException {
    if (context.getConf().yearIsDateType()) {
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
  public Object getDefaultBinary(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException, IOException {
    if (context.getConf().yearIsDateType()) {
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
  public Date decodeDateText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
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
  public Date decodeDateBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
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
  public Timestamp decodeTimestampText(
      final ReadableByteBuf buf, final MutableInt length, Calendar calParam, final Context context)
      throws SQLDataException, IOException {
    int year = Integer.parseInt(buf.readAscii(length.get()));
    if (columnLength <= 2) year += year >= 70 ? 1900 : 2000;

    if (calParam == null) {
      Calendar cal1 = context.getDefaultCalendar();
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
  public Timestamp decodeTimestampBinary(
      final ReadableByteBuf buf, final MutableInt length, Calendar calParam, final Context context)
      throws SQLDataException, IOException {
    int year = buf.readUnsignedShort();
    if (columnLength <= 2) year += year >= 70 ? 1900 : 2000;

    Timestamp timestamp;
    if (calParam == null) {
      Calendar cal = context.getDefaultCalendar();
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
