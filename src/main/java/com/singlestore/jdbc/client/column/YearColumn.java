// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client.column;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.util.MutableInt;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

/** Column metadata definition */
public class YearColumn extends UnsignedSmallIntColumn {

  /**
   * YEAR metadata type decoder
   *
   * @param buf buffer
   * @param charset charset
   * @param length maximum data length
   * @param dataType data type
   * @param decimals decimal length
   * @param flags flags
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

  protected YearColumn(YearColumn prev) {
    super(prev);
  }

  @Override
  public YearColumn useAliasAsName() {
    return new YearColumn(this);
  }

  @Override
  public String defaultClassname(Configuration conf) {
    return conf.yearIsDateType() ? Date.class.getName() : Short.class.getName();
  }

  @Override
  public int getColumnType(Configuration conf) {
    return conf.yearIsDateType() ? Types.DATE : Types.SMALLINT;
  }

  @Override
  public String getColumnTypeName(Configuration conf) {
    return dataType.name();
  }

  @Override
  public int getPrecision() {
    return (int) Math.max(getColumnLength(), 0);
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    if (conf.yearIsDateType()) {
      return decodeDateText(buf, length, null);
    }
    return decodeShortText(buf, length);
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    if (conf.yearIsDateType()) {
      return decodeDateBinary(buf, length, null);
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
    Calendar cal1 = calParam == null ? Calendar.getInstance() : calParam;

    int year = Integer.parseInt(buf.readAscii(length.get()));
    if (columnLength <= 2) year += year >= 70 ? 1900 : 2000;
    synchronized (cal1) {
      cal1.clear();
      cal1.set(year, Calendar.JANUARY, 1);
      return new Timestamp(cal1.getTimeInMillis());
    }
  }

  @Override
  public Timestamp decodeTimestampBinary(ReadableByteBuf buf, MutableInt length, Calendar calParam)
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
