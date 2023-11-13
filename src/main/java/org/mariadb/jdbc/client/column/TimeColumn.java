// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import java.sql.*;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.plugin.codec.LocalTimeCodec;

/** Column metadata definition */
public class TimeColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  /**
   * TIME metadata type decoder
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
  public TimeColumn(
      ReadableByteBuf buf,
      int charset,
      long length,
      DataType dataType,
      byte decimals,
      int flags,
      int[] stringPos,
      String extTypeName,
      String extTypeFormat) {
    super(buf, charset, length, dataType, decimals, flags, stringPos, extTypeName, extTypeFormat, false);
  }
  protected TimeColumn(TimeColumn prev) {
    super(prev, true);
  }

  @Override
  public TimeColumn useAliasAsName() {
    return new TimeColumn(this);
  }
  public String defaultClassname(Configuration conf) {
    return Time.class.getName();
  }

  public int getColumnType(Configuration conf) {
    return Types.TIME;
  }

  public String getColumnTypeName(Configuration conf) {
    return "TIME";
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    Calendar c = Calendar.getInstance();
    int offset = c.getTimeZone().getOffset(0);
    int[] parts = LocalTimeCodec.parseTime(buf, length, this);
    long timeInMillis =
        (parts[1] * 3_600_000L + parts[2] * 60_000L + parts[3] * 1_000L + parts[4] / 1_000_000)
                * parts[0]
            - offset;
    return new Time(timeInMillis);
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    boolean negate = false;
    Calendar cal = Calendar.getInstance();
    long dayOfMonth = 0;
    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;
    if (length.get() > 0) {
      // specific case for TIME, to handle value not in 00:00:00-23:59:59
      negate = buf.readByte() == 1;
      dayOfMonth = buf.readUnsignedInt();
      hour = buf.readByte();
      minutes = buf.readByte();
      seconds = buf.readByte();
      if (length.get() > 8) {
        microseconds = buf.readUnsignedInt();
      }
    }
    int offset = cal.getTimeZone().getOffset(0);
    long timeInMillis =
        ((24 * dayOfMonth + hour) * 3_600_000
                    + minutes * 60_000
                    + seconds * 1_000
                    + microseconds / 1_000)
                * (negate ? -1 : 1)
            - offset;
    return new Time(timeInMillis);
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Byte", dataType));
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Byte", dataType));
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Boolean", dataType));
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Boolean", dataType));
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    long tDays = 0;
    int tHours = 0;
    int tMinutes = 0;
    int tSeconds = 0;
    long tMicroseconds = 0;
    if (length.get() == 0) {
      StringBuilder zeroValue = new StringBuilder("00:00:00");
      if (getDecimals() > 0) {
        zeroValue.append(".");
        for (int i = 0; i < getDecimals(); i++) zeroValue.append("0");
      }
      return zeroValue.toString();
    }
    boolean negate = buf.readByte() == 0x01;
    if (length.get() > 4) {
      tDays = buf.readUnsignedInt();
      if (length.get() > 7) {
        tHours = buf.readByte();
        tMinutes = buf.readByte();
        tSeconds = buf.readByte();
        if (length.get() > 8) {
          tMicroseconds = buf.readInt();
        }
      }
    }
    int totalHour = (int) (tDays * 24 + tHours);
    String stTime =
        (negate ? "-" : "")
            + (totalHour < 10 ? "0" : "")
            + totalHour
            + ":"
            + (tMinutes < 10 ? "0" : "")
            + tMinutes
            + ":"
            + (tSeconds < 10 ? "0" : "")
            + tSeconds;
    if (getDecimals() == 0) {
      if (tMicroseconds == 0) return stTime;
      // possible for Xpand that doesn't send some metadata
      // https://jira.mariadb.org/browse/XPT-273
      StringBuilder stMicro = new StringBuilder(String.valueOf(tMicroseconds));
      while (stMicro.length() < 6) {
        stMicro.insert(0, "0");
      }
      return stTime + "." + stMicro;
    }
    StringBuilder stMicro = new StringBuilder(String.valueOf(tMicroseconds));
    while (stMicro.length() < getDecimals()) {
      stMicro.insert(0, "0");
    }
    return stTime + "." + stMicro;
  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Short", dataType));
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Short", dataType));
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Integer", dataType));
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Integer", dataType));
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Float", dataType));
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Float", dataType));
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Double", dataType));
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Double", dataType));
  }

  @Override
  public Date decodeDateText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
  }

  @Override
  public Date decodeDateBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
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
    long dayOfMonth = 0;
    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;
    boolean negate = false;
    if (length.get() > 0) {
      // specific case for TIME, to handle value not in 00:00:00-23:59:59
      negate = buf.readByte() == 1;
      dayOfMonth = buf.readUnsignedInt();
      hour = buf.readByte();
      minutes = buf.readByte();
      seconds = buf.readByte();
      if (length.get() > 8) {
        microseconds = buf.readUnsignedInt();
      }
    }
    int offset = cal.getTimeZone().getOffset(0);
    long timeInMillis =
        ((24 * dayOfMonth + hour) * 3_600_000
                    + minutes * 60_000
                    + seconds * 1_000
                    + microseconds / 1_000)
                * (negate ? -1 : 1)
            - offset;
    return new Time(timeInMillis);
  }

  @Override
  public Timestamp decodeTimestampText(ReadableByteBuf buf, MutableInt length, Calendar calParam)
      throws SQLDataException {
    int[] parts = LocalTimeCodec.parseTime(buf, length, this);
    Timestamp t;

    // specific case for TIME, to handle value not in 00:00:00-23:59:59
    if (calParam == null) {
      Calendar cal = Calendar.getInstance();
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
        t = new Timestamp(cal.getTimeInMillis());
        t.setNanos(1_000_000_000 - parts[4]);
      } else {
        cal.set(1970, Calendar.JANUARY, 1, parts[1], parts[2], parts[3]);
        t = new Timestamp(cal.getTimeInMillis());
        t.setNanos(parts[4]);
      }
    } else {
      synchronized (calParam) {
        calParam.clear();
        calParam.setLenient(true);
        if (parts[0] == -1) {
          calParam.set(
              1970,
              Calendar.JANUARY,
              1,
              parts[0] * parts[1],
              parts[0] * parts[2],
              parts[0] * parts[3] - 1);
          t = new Timestamp(calParam.getTimeInMillis());
          t.setNanos(1_000_000_000 - parts[4]);
        } else {
          calParam.set(1970, Calendar.JANUARY, 1, parts[1], parts[2], parts[3]);
          t = new Timestamp(calParam.getTimeInMillis());
          t.setNanos(parts[4]);
        }
      }
    }
    return t;
  }

  @Override
  public Timestamp decodeTimestampBinary(ReadableByteBuf buf, MutableInt length, Calendar calParam)
      throws SQLDataException {
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
    long microseconds = 0;

    // specific case for TIME, to handle value not in 00:00:00-23:59:59
    boolean negate = buf.readByte() == 1;
    long dayOfMonth = buf.readUnsignedInt();
    int hour = buf.readByte();
    int minutes = buf.readByte();
    int seconds = buf.readByte();
    if (length.get() > 8) {
      microseconds = buf.readUnsignedInt();
    }
    int offset = cal.getTimeZone().getOffset(0);
    long timeInMillis =
        ((24 * dayOfMonth + hour) * 3_600_000
                    + minutes * 60_000
                    + seconds * 1_000
                    + microseconds / 1_000)
                * (negate ? -1 : 1)
            - offset;
    return new Timestamp(timeInMillis);
  }
}
