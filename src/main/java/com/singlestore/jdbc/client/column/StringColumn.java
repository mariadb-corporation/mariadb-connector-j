// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client.column;

import static com.singlestore.jdbc.client.result.Result.NULL_LENGTH;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.client.ColumnDecoder;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.util.MutableInt;
import com.singlestore.jdbc.message.server.ColumnDefinitionPacket;
import com.singlestore.jdbc.plugin.codec.LocalDateTimeCodec;
import com.singlestore.jdbc.plugin.codec.LocalTimeCodec;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.DateTimeException;
import java.util.Calendar;

/** Column metadata definition */
public class StringColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  /**
   * VARCHAR/STRING/VARSTRING metadata type decoder
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
  public StringColumn(
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

  @Override
  public String defaultClassname(Configuration conf) {
    return String.class.getName();
  }

  @Override
  public int getColumnType(Configuration conf) {
    if (dataType == DataType.NULL) {
      return Types.NULL;
    }
    if (dataType == DataType.CHAR) {
      return isBinary() ? Types.BINARY : Types.CHAR;
    }
    return isBinary() ? Types.VARBINARY : Types.VARCHAR;
  }

  @Override
  public String getColumnTypeName(Configuration conf) {
    switch (dataType) {
      case CHAR:
        return isBinary() ? "BINARY" : "CHAR";
      case VARCHAR:
        return isBinary() ? "VARBINARY" : "VARCHAR";
      default:
        return dataType.name();
    }
  }

  @Override
  public int getPrecision() {
    if (dataType != DataType.NULL) {
      return getDisplaySize();
    }
    return ColumnDecoder.super.getPrecision();
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return !"0".equals(buf.readAscii(length.get()));
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return !"0".equals(buf.readAscii(length.get()));
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    String str = buf.readString(length.get());
    long result;
    try {
      result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValue();
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(
          String.format("value '%s' (%s) cannot be decoded as Byte", str, dataType));
    }
    if ((byte) result != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("byte overflow");
    }
    return (byte) result;
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return decodeByteText(buf, length);
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    String str = buf.readString(length.get());
    try {
      return new BigDecimal(str).setScale(0, RoundingMode.DOWN).shortValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Short", str));
    }
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return decodeShortText(buf, length);
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    String str = buf.readString(length.get());
    try {
      return new BigDecimal(str).setScale(0, RoundingMode.DOWN).intValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Integer", str));
    }
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return decodeIntText(buf, length);
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    String str = buf.readString(length.get());
    try {
      return new BigInteger(str).longValueExact();
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str));
    }
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return decodeLongText(buf, length);
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    String val = buf.readString(length.get());
    try {
      return Float.parseFloat(val);
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Float", val));
    }
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return decodeFloatText(buf, length);
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    String str2 = buf.readString(length.get());
    try {
      return Double.parseDouble(str2);
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Double", str2));
    }
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return decodeDoubleText(buf, length);
  }

  @Override
  public Date decodeDateText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    String val = buf.readString(length.get());
    if ("0000-00-00".equals(val)) return null;
    String[] stDatePart = val.split("[- ]");
    if (stDatePart.length < 3) {
      throw new SQLDataException(
          String.format("value '%s' (%s) cannot be decoded as Date", val, dataType));
    }

    try {
      int year = Integer.parseInt(stDatePart[0]);
      int month = Integer.parseInt(stDatePart[1]);
      int dayOfMonth = Integer.parseInt(stDatePart[2]);
      Calendar c = cal == null ? Calendar.getInstance() : cal;
      synchronized (c) {
        c.clear();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.MONTH, month - 1);
        c.set(Calendar.DAY_OF_MONTH, dayOfMonth);
        return new Date(c.getTimeInMillis());
      }

    } catch (NumberFormatException nfe) {
      throw new SQLDataException(
          String.format("value '%s' (%s) cannot be decoded as Date", val, dataType));
    }
  }

  @Override
  public Date decodeDateBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    return decodeDateText(buf, length, cal);
  }

  @Override
  public Time decodeTimeText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    Calendar c = cal == null ? Calendar.getInstance() : cal;
    int offset = c.getTimeZone().getOffset(0);
    int[] parts = LocalTimeCodec.parseTime(buf, length, this);
    long timeInMillis =
        (parts[1] * 3_600_000L + parts[2] * 60_000L + parts[3] * 1_000L + parts[4] / 1_000_000)
                * parts[0]
            - offset;
    return new Time(timeInMillis);
  }

  @Override
  public Time decodeTimeBinary(ReadableByteBuf buf, MutableInt length, Calendar calParam)
      throws SQLDataException {
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
    int[] parts = LocalTimeCodec.parseTime(buf, length, this);
    Time t;

    // specific case for TIME, to handle value not in 00:00:00-23:59:59
    synchronized (cal) {
      cal.clear();
      cal.setLenient(true);
      if (parts[0] == -1) {
        cal.set(
            1970,
            Calendar.JANUARY,
            1,
            parts[0] * parts[1],
            parts[0] * parts[2],
            parts[0] * parts[3] - 1);
        t = new Time(cal.getTimeInMillis() + (1000 - parts[4]));
      } else {
        cal.set(1970, Calendar.JANUARY, 1, parts[1], parts[2], parts[3]);
        t = new Time(cal.getTimeInMillis() + parts[4] / 1_000_000);
      }
    }
    return t;
  }

  @Override
  public Timestamp decodeTimestampText(ReadableByteBuf buf, MutableInt length, Calendar calParam)
      throws SQLDataException {
    int pos = buf.pos();
    int nanoBegin = -1;
    int[] timestampsPart = new int[] {0, 0, 0, 0, 0, 0, 0};
    int partIdx = 0;
    for (int begin = 0; begin < length.get(); begin++) {
      byte b = buf.readByte();
      if (b == '-' || b == ' ' || b == ':') {
        partIdx++;
        continue;
      }
      if (b == '.') {
        partIdx++;
        nanoBegin = begin;
        continue;
      }
      if (b < '0' || b > '9') {
        buf.pos(pos);
        throw new SQLDataException(
            String.format(
                "value '%s' (%s) cannot be decoded as Timestamp",
                buf.readString(length.get()), dataType));
      }

      timestampsPart[partIdx] = timestampsPart[partIdx] * 10 + b - 48;
    }
    if (timestampsPart[0] == 0
        && timestampsPart[1] == 0
        && timestampsPart[2] == 0
        && timestampsPart[3] == 0
        && timestampsPart[4] == 0
        && timestampsPart[5] == 0
        && timestampsPart[6] == 0) {
      length.set(NULL_LENGTH);
      return null;
    }

    // fix non-leading tray for nanoseconds
    if (nanoBegin > 0) {
      for (int begin = 0; begin < 6 - (length.get() - nanoBegin - 1); begin++) {
        timestampsPart[6] = timestampsPart[6] * 10;
      }
    }

    Timestamp timestamp;
    if (calParam == null) {
      Calendar c = Calendar.getInstance();
      c.set(
          timestampsPart[0],
          timestampsPart[1] - 1,
          timestampsPart[2],
          timestampsPart[3],
          timestampsPart[4],
          timestampsPart[5]);
      timestamp = new Timestamp(c.getTime().getTime());
      timestamp.setNanos(timestampsPart[6] * 1000);
    } else {
      synchronized (calParam) {
        calParam.clear();
        calParam.set(
            timestampsPart[0],
            timestampsPart[1] - 1,
            timestampsPart[2],
            timestampsPart[3],
            timestampsPart[4],
            timestampsPart[5]);
        timestamp = new Timestamp(calParam.getTime().getTime());
        timestamp.setNanos(timestampsPart[6] * 1000);
      }
    }
    return timestamp;
  }

  @Override
  public Timestamp decodeTimestampBinary(ReadableByteBuf buf, MutableInt length, Calendar calParam)
      throws SQLDataException {
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;

    String val = buf.readString(length.get());
    try {
      int[] parts = LocalDateTimeCodec.parseTimestamp(val);
      if (parts == null) {
        length.set(NULL_LENGTH);
        return null;
      }
      int year = parts[0];
      int month = parts[1];
      int dayOfMonth = parts[2];
      int hour = parts[3];
      int minutes = parts[4];
      int seconds = parts[5];
      int microseconds = parts[6] / 1000;
      Timestamp timestamp;
      synchronized (cal) {
        cal.clear();
        cal.set(year, month - 1, dayOfMonth, hour, minutes, seconds);
        timestamp = new Timestamp(cal.getTimeInMillis());
      }
      timestamp.setNanos(microseconds * 1000);
      return timestamp;

    } catch (DateTimeException dte) {
      throw new SQLDataException(
          String.format("value '%s' (%s) cannot be decoded as Timestamp", val, dataType));
    }
  }
}
