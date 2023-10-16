// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import static org.mariadb.jdbc.client.result.Result.NULL_LENGTH;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.plugin.codec.LocalDateTimeCodec;
import org.mariadb.jdbc.plugin.codec.TimeCodec;

/** Column metadata definition */
public class TimestampColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  /**
   * TIMESTAMP metadata type decoder
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
  public TimestampColumn(
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
    return Timestamp.class.getName();
  }

  public int getColumnType(Configuration conf) {
    return Types.TIMESTAMP;
  }

  public String getColumnTypeName(Configuration conf) {
    return dataType.name();
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return decodeTimestampText(buf, length, null);
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return decodeTimestampBinary(buf, length, null);
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
  public String decodeStringText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    if (length.get() == 0) {
      StringBuilder zeroValue = new StringBuilder("0000-00-00 00:00:00");
      if (getDecimals() > 0) {
        zeroValue.append(".");
        for (int i = 0; i < getDecimals(); i++) zeroValue.append("0");
      }
      return zeroValue.toString();
    }
    int year = buf.readUnsignedShort();
    int month = buf.readByte();
    int day = buf.readByte();
    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;

    if (length.get() > 4) {
      hour = buf.readByte();
      minutes = buf.readByte();
      seconds = buf.readByte();

      if (length.get() > 7) {
        microseconds = buf.readUnsignedInt();
      }
    }

    // xpand workaround https://jira.mariadb.org/browse/XPT-274
    if (year == 0 && month == 0 && day == 0) {
      return "0000-00-00 00:00:00";
    }

    LocalDateTime dateTime =
        LocalDateTime.of(year, month, day, hour, minutes, seconds).plusNanos(microseconds * 1000);

    StringBuilder microSecPattern = new StringBuilder();
    if (getDecimals() > 0 || microseconds > 0) {
      int decimal = getDecimals() & 0xff;
      if (decimal == 0) decimal = 6;
      microSecPattern.append(".");
      for (int i = 0; i < decimal; i++) microSecPattern.append("S");
    }
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss" + microSecPattern);
    return dateTime.toLocalDate().toString() + ' ' + dateTime.toLocalTime().format(formatter);
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
    if (cal == null) {
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
      synchronized (cal) {
        cal.clear();
        cal.set(
            timestampsPart[0],
            timestampsPart[1] - 1,
            timestampsPart[2],
            timestampsPart[3],
            timestampsPart[4],
            timestampsPart[5]);
        timestamp = new Timestamp(cal.getTime().getTime());
        timestamp.setNanos(timestampsPart[6] * 1000);
      }
    }

    String st = timestamp.toString();
    return Date.valueOf(st.substring(0, 10));
  }

  @Override
  public Date decodeDateBinary(ReadableByteBuf buf, MutableInt length, Calendar calParam)
      throws SQLDataException {
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;

    if (length.get() == 0) {
      length.set(NULL_LENGTH);
      return null;
    }
    int year = buf.readUnsignedShort();
    int month = buf.readByte();
    int dayOfMonth = buf.readByte();
    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;

    if (length.get() > 4) {
      hour = buf.readByte();
      minutes = buf.readByte();
      seconds = buf.readByte();

      if (length.get() > 7) {
        microseconds = buf.readUnsignedInt();
      }
    }

    // xpand workaround https://jira.mariadb.org/browse/XPT-274
    if (year == 0
        && month == 0
        && dayOfMonth == 0
        && hour == 0
        && minutes == 0
        && seconds == 0
        && microseconds == 0) {
      length.set(NULL_LENGTH);
      return null;
    }

    Timestamp timestamp;
    synchronized (cal) {
      cal.clear();
      cal.set(year, month - 1, dayOfMonth, hour, minutes, seconds);
      timestamp = new Timestamp(cal.getTimeInMillis());
    }
    timestamp.setNanos((int) (microseconds * 1000));
    String st = timestamp.toString();
    return Date.valueOf(st.substring(0, 10));
  }

  @Override
  public Time decodeTimeText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    LocalDateTime lt = LocalDateTimeCodec.INSTANCE.decodeText(buf, length, this, cal);
    if (lt == null) return null;
    Calendar cc = cal == null ? Calendar.getInstance() : cal;
    ZonedDateTime d =
        TimeCodec.EPOCH_DATE.atTime(lt.toLocalTime()).atZone(cc.getTimeZone().toZoneId());
    return new Time(d.toEpochSecond() * 1000 + d.getNano() / 1_000_000);
  }

  @Override
  public Time decodeTimeBinary(ReadableByteBuf buf, MutableInt length, Calendar calParam)
      throws SQLDataException {
    if (length.get() == 0) {
      length.set(NULL_LENGTH);
      return null;
    }
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;

    int year = buf.readUnsignedShort();
    int month = buf.readByte();
    int dayOfMonth = buf.readByte();

    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;

    if (length.get() > 4) {
      hour = buf.readByte();
      minutes = buf.readByte();
      seconds = buf.readByte();

      if (length.get() > 7) {
        microseconds = buf.readUnsignedInt();
      }
    }

    if (year == 0 && month == 0 && dayOfMonth == 0 && hour == 0 && minutes == 0 && seconds == 0) {
      length.set(NULL_LENGTH);
      return null;
    }

    synchronized (cal) {
      cal.clear();
      cal.set(1970, Calendar.JANUARY, 1, hour, minutes, seconds);
      return new Time(cal.getTimeInMillis() + microseconds / 1_000);
    }
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
    if (length.get() == 0) {
      length.set(NULL_LENGTH);
      return null;
    }
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;

    int year = buf.readUnsignedShort();
    int month = buf.readByte();
    int dayOfMonth = buf.readByte();
    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;

    if (length.get() > 4) {
      hour = buf.readByte();
      minutes = buf.readByte();
      seconds = buf.readByte();

      if (length.get() > 7) {
        microseconds = buf.readUnsignedInt();
      }
    }

    // xpand workaround https://jira.mariadb.org/browse/XPT-274
    if (year == 0
        && month == 0
        && dayOfMonth == 0
        && hour == 0
        && minutes == 0
        && seconds == 0
        && microseconds == 0) {
      length.set(NULL_LENGTH);
      return null;
    }
    Timestamp timestamp;
    synchronized (cal) {
      cal.clear();
      cal.set(year, month - 1, dayOfMonth, hour, minutes, seconds);
      timestamp = new Timestamp(cal.getTimeInMillis());
    }
    timestamp.setNanos((int) (microseconds * 1000));
    return timestamp;
  }
}
