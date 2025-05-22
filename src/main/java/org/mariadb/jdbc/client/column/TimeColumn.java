// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import java.io.IOException;
import java.sql.*;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.impl.readable.BufferedReadableByteBuf;
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
      final BufferedReadableByteBuf buf,
      final int charset,
      final long length,
      final DataType dataType,
      final byte decimals,
      final int flags,
      final int[] stringPos,
      final String extTypeName,
      final String extTypeFormat) {
    super(
        buf,
        charset,
        length,
        dataType,
        decimals,
        flags,
        stringPos,
        extTypeName,
        extTypeFormat,
        false);
  }

  /**
   * Recreate new column using alias as name.
   *
   * @param prev current column
   */
  protected TimeColumn(TimeColumn prev) {
    super(prev, true);
  }

  @Override
  public TimeColumn useAliasAsName() {
    return new TimeColumn(this);
  }

  public String defaultClassname(final Configuration conf) {
    return Time.class.getName();
  }

  public int getColumnType(final Configuration conf) {
    return Types.TIME;
  }

  public String getColumnTypeName(final Configuration conf) {
    return "TIME";
  }

  @Override
  public Object getDefaultText(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException, IOException {
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
  public Object getDefaultBinary(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException, IOException {
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
  public byte decodeByteText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Byte", dataType));
  }

  @Override
  public byte decodeByteBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Byte", dataType));
  }

  @Override
  public boolean decodeBooleanText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Boolean", dataType));
  }

  @Override
  public boolean decodeBooleanBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Boolean", dataType));
  }

  @Override
  public String decodeStringText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    return buf.readString(length.get());
  }

  @Override
  public String decodeStringBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {

    if (length.get() == 0) {
      return createZeroTimeString();
    }

    boolean negate = buf.readByte() == 0x01;
    long days = 0;
    int hours = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;

    if (length.get() > 4) {
      days = buf.readUnsignedInt();
      if (length.get() > 7) {
        hours = buf.readByte();
        minutes = buf.readByte();
        seconds = buf.readByte();
        if (length.get() > 8) {
          microseconds = buf.readInt();
        }
      }
    }

    String timeString = formatBasicTimeString(negate, days, hours, minutes, seconds);

    return formatWithMicroseconds(timeString, microseconds);
  }

  private String createZeroTimeString() {
    StringBuilder zeroValue = new StringBuilder("00:00:00");
    if (getDecimals() > 0) {
      zeroValue.append(".");
      for (int i = 0; i < getDecimals(); i++) zeroValue.append("0");
    }
    return zeroValue.toString();
  }

  private String formatBasicTimeString(
      boolean negate, long days, int hours, int minutes, int seconds) {
    int totalHours = (int) (days * 24 + hours);

    return String.format("%s%02d:%02d:%02d", negate ? "-" : "", totalHours, minutes, seconds);
  }

  private String formatWithMicroseconds(String timeString, long microseconds) {
    if (getDecimals() == 0) {
      if (microseconds == 0) {
        return timeString;
      }
      // Handle Xpand case that doesn't send some metadata
      // https://jira.mariadb.org/browse/XPT-273
      return timeString + "." + padZeros(microseconds, 6);
    }

    return timeString + "." + padZeros(microseconds, getDecimals());
  }

  private String padZeros(long number, int targetLength) {
    StringBuilder result = new StringBuilder(String.valueOf(number));
    while (result.length() < targetLength) {
      result.insert(0, "0");
    }
    return result.toString();
  }

  @Override
  public short decodeShortText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Short", dataType));
  }

  @Override
  public short decodeShortBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Short", dataType));
  }

  @Override
  public int decodeIntText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Integer", dataType));
  }

  @Override
  public int decodeIntBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Integer", dataType));
  }

  @Override
  public long decodeLongText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
  }

  @Override
  public long decodeLongBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
  }

  @Override
  public float decodeFloatText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Float", dataType));
  }

  @Override
  public float decodeFloatBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Float", dataType));
  }

  @Override
  public double decodeDoubleText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Double", dataType));
  }

  @Override
  public double decodeDoubleBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Double", dataType));
  }

  @Override
  public Date decodeDateText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
  }

  @Override
  public Date decodeDateBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
  }

  @Override
  public Time decodeTimeText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
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
  public Time decodeTimeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final Calendar calParam,
      final Context context)
      throws SQLDataException, IOException {
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
  public Timestamp decodeTimestampText(
      final ReadableByteBuf buf, final MutableInt length, Calendar calParam, final Context context)
      throws SQLDataException, IOException {
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
  public Timestamp decodeTimestampBinary(
      final ReadableByteBuf buf, final MutableInt length, Calendar calParam, final Context context)
      throws SQLDataException, IOException {
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
