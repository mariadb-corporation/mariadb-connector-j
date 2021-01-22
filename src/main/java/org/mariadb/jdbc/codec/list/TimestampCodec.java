/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.codec.list;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.TimeZone;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class TimestampCodec implements Codec<Timestamp> {

  public static final TimestampCodec INSTANCE = new TimestampCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.DATE,
          DataType.NEWDATE,
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.YEAR,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING,
          DataType.TIME);

  public String className() {
    return Timestamp.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Timestamp.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Timestamp;
  }

  @Override
  public Timestamp decodeText(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar calParam)
      throws SQLDataException {

    switch (column.getType()) {
      case TIME:
        int[] parts = LocalTimeCodec.parseTime(buf, length, column);
        Timestamp t;

        // specific case for TIME, to handle value not in 00:00:00-23:59:59
        Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
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
            t = new Timestamp(cal.getTimeInMillis());
            t.setNanos(1_000_000_000 - parts[4]);
          } else {
            cal.set(1970, Calendar.JANUARY, 1, parts[1], parts[2], parts[3]);
            t = new Timestamp(cal.getTimeInMillis());
            t.setNanos(parts[4]);
          }
        }
        return t;

      case DATE:
        if (calParam == null || calParam.getTimeZone().equals(TimeZone.getDefault())) {
          return new Timestamp(Date.valueOf(buf.readAscii(length)).getTime());
        }

        String[] datePart = buf.readAscii(length).split("-");
        synchronized (calParam) {
          calParam.clear();
          calParam.set(
              Integer.parseInt(datePart[0]),
              Integer.parseInt(datePart[1]) - 1,
              Integer.parseInt(datePart[2]));
          return new Timestamp(calParam.getTimeInMillis());
        }

      case STRING:
      case VARCHAR:
      case VARSTRING:
      case TIMESTAMP:
      case DATETIME:
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
                    buf.readString(length), column.getType()));
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

        // fix non leading tray for nanoseconds
        if (nanoBegin > 0) {
          for (int begin = 0; begin < 6 - (length - nanoBegin - 1); begin++) {
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

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Timestamp", column.getType()));
    }
  }

  @Override
  public Timestamp decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar calParam)
      throws SQLDataException {
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
    int year;
    int month;
    long dayOfMonth;
    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;

    switch (column.getType()) {
      case TIME:
        // specific case for TIME, to handle value not in 00:00:00-23:59:59
        boolean negate = buf.readByte() == 1;
        dayOfMonth = buf.readUnsignedInt();
        hour = buf.readByte();
        minutes = buf.readByte();
        seconds = buf.readByte();
        if (length > 8) {
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

      case STRING:
      case VARCHAR:
      case VARSTRING:
        String val = buf.readString(length);
        try {
          int[] parts = LocalDateTimeCodec.parseTimestamp(val);
          if (parts == null) return null;
          year = parts[0];
          month = parts[1];
          dayOfMonth = parts[2];
          hour = parts[3];
          minutes = parts[4];
          seconds = parts[5];
          microseconds = parts[6] / 1000;
          break;
        } catch (DateTimeException dte) {
          throw new SQLDataException(
              String.format(
                  "value '%s' (%s) cannot be decoded as Timestamp", val, column.getType()));
        }

      case DATE:
      case TIMESTAMP:
      case DATETIME:
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
        break;

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Timestamp", column.getType()));
    }
    Timestamp timestamp;
    synchronized (cal) {
      cal.clear();
      cal.set(year, month - 1, (int) dayOfMonth, hour, minutes, seconds);
      timestamp = new Timestamp(cal.getTimeInMillis());
    }
    timestamp.setNanos((int) (microseconds * 1000));
    return timestamp;
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Object val, Calendar providedCal, Long maxLen)
      throws IOException {
    Calendar cal = providedCal == null ? Calendar.getInstance() : providedCal;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(cal.getTimeZone());
    String dateString = sdf.format(val);

    encoder.writeByte('\'');
    encoder.writeAscii(dateString);
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(
      PacketWriter encoder, Context context, Object value, Calendar providedCal, Long maxLength)
      throws IOException {
    Calendar cal = providedCal == null ? Calendar.getInstance() : providedCal;
    cal.setTimeInMillis(((Timestamp) value).getTime());
    if (cal.get(Calendar.MILLISECOND) == 0) {
      encoder.writeByte(7); // length
      encoder.writeShort((short) cal.get(Calendar.YEAR));
      encoder.writeByte((cal.get(Calendar.MONTH) + 1));
      encoder.writeByte(cal.get(Calendar.DAY_OF_MONTH));
      encoder.writeByte(cal.get(Calendar.HOUR_OF_DAY));
      encoder.writeByte(cal.get(Calendar.MINUTE));
      encoder.writeByte(cal.get(Calendar.SECOND));
    } else {
      encoder.writeByte(11); // length
      encoder.writeShort((short) cal.get(Calendar.YEAR));
      encoder.writeByte((cal.get(Calendar.MONTH) + 1));
      encoder.writeByte(cal.get(Calendar.DAY_OF_MONTH));
      encoder.writeByte(cal.get(Calendar.HOUR_OF_DAY));
      encoder.writeByte(cal.get(Calendar.MINUTE));
      encoder.writeByte(cal.get(Calendar.SECOND));
      encoder.writeInt(cal.get(Calendar.MILLISECOND) * 1000);
    }
  }

  public int getBinaryEncodeType() {
    return DataType.DATETIME.get();
  }
}
