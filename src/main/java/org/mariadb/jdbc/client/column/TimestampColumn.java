// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.column;

import java.sql.SQLDataException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

/** Column metadata definition */
public class TimestampColumn extends ColumnDefinitionPacket implements ColumnDecoder {

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
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, int length)
      throws SQLDataException {
    int pos = buf.pos();
    int nanoBegin = -1;
    int[] timestampsPart = new int[] {0, 0, 0, 0, 0, 0, 0};
    int partIdx = 0;
    for (int begin = 0; begin < length; begin++) {
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
                buf.readString(length), dataType));
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
      return null;
    }

    // fix non-leading tray for nanoseconds
    if (nanoBegin > 0) {
      for (int begin = 0; begin < 6 - (length - nanoBegin - 1); begin++) {
        timestampsPart[6] = timestampsPart[6] * 10;
      }
    }

    Calendar c = Calendar.getInstance();
    c.set(
        timestampsPart[0],
        timestampsPart[1] - 1,
        timestampsPart[2],
        timestampsPart[3],
        timestampsPart[4],
        timestampsPart[5]);
    Timestamp timestamp = new Timestamp(c.getTime().getTime());
    timestamp.setNanos(timestampsPart[6] * 1000);
    return timestamp;
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, int length)
      throws SQLDataException {
    if (length == 0) return null;
    Calendar cal = Calendar.getInstance();
    int year;
    int month = 1;
    long dayOfMonth = 1;
    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;
    year = buf.readUnsignedShort();
    month = buf.readByte();
    dayOfMonth = buf.readByte();

    if (length > 4) {
      hour = buf.readByte();
      minutes = buf.readByte();
      seconds = buf.readByte();

      if (length > 7) {
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
        && microseconds == 0) return null;
    Timestamp timestamp;
    cal.clear();
    cal.set(year, month - 1, (int) dayOfMonth, hour, minutes, seconds);
    timestamp = new Timestamp(cal.getTimeInMillis());
    timestamp.setNanos((int) (microseconds * 1000));
    return timestamp;
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Boolean", dataType));
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Boolean", dataType));
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Byte", dataType));
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Byte", dataType));
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length);
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    if (length == 0) {
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

    if (length > 4) {
      hour = buf.readByte();
      minutes = buf.readByte();
      seconds = buf.readByte();

      if (length > 7) {
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
  public short decodeShortText(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Short", dataType));
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Short", dataType));
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Integer", dataType));
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Integer", dataType));
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Float", dataType));
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Float", dataType));
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Double", dataType));
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Double", dataType));
  }
}
