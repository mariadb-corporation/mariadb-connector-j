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
import java.sql.SQLDataException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class TimeCodec implements Codec<Time> {
  public static final TimeCodec INSTANCE = new TimeCodec();
  private static final LocalDate EPOCH_DATE = LocalDate.of(1970, 1, 1);
  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TIME,
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING);

  public String className() {
    return Time.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && type.isAssignableFrom(Time.class)
        && !type.equals(java.util.Date.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Time;
  }

  @Override
  public Time decodeText(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {

    switch (column.getType()) {
      case VARCHAR:
      case VARSTRING:
      case STRING:
      case TIME:
        Calendar c = cal == null ? Calendar.getInstance() : cal;
        int offset = c.getTimeZone().getOffset(0);
        int[] parts = LocalTimeCodec.parseTime(buf, length, column);
        long timeInMillis =
            (parts[1] * 3_600_000 + parts[2] * 60_000 + parts[3] * 1_000 + parts[4] / 1_000_000)
                    * parts[0]
                - offset;
        return new Time(timeInMillis);

      case TIMESTAMP:
      case DATETIME:
        LocalDateTime lt = LocalDateTimeCodec.INSTANCE.decodeText(buf, length, column, cal);
        Calendar cc = cal == null ? Calendar.getInstance() : cal;
        ZonedDateTime d = EPOCH_DATE.atTime(lt.toLocalTime()).atZone(cc.getTimeZone().toZoneId());
        return new Time(d.toEpochSecond() * 1000 + d.getNano() / 1_000_000);

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Time", column.getType()));
    }
  }

  @Override
  public Time decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar calParam)
      throws SQLDataException {

    switch (column.getType()) {
      case VARCHAR:
      case VARSTRING:
      case STRING:
      case TIME:
        int[] parts = LocalTimeCodec.parseTime(buf, length, column);
        Time t;

        // specific case for TIME, to handle value not in 00:00:00-23:59:59
        Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
        synchronized (cal) {
          cal.clear();
          cal.setLenient(true);
          if (parts[0] == -1) {
            cal.set(1970, 0, 1, parts[0] * parts[1], parts[0] * parts[2], parts[0] * parts[3] - 1);
            t = new Time(cal.getTimeInMillis() + (1000 - parts[4]));
          } else {
            cal.set(1970, 0, 1, parts[1], parts[2], parts[3]);
            t = new Time(cal.getTimeInMillis() + parts[4] / 1_000_000);
          }
        }
        return t;

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

        Time time;
        if (calParam == null) {
          Calendar c = Calendar.getInstance();
          c.set(
              timestampsPart[0],
              timestampsPart[1] - 1,
              timestampsPart[2],
              timestampsPart[3],
              timestampsPart[4],
              timestampsPart[5]);
          time = new Time(c.getTime().getTime() + timestampsPart[6] / 1000);
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
            time = new Time(calParam.getTime().getTime() + timestampsPart[6] / 1000);
          }
        }
        return time;

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Time", column.getType()));
    }
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Time val, Calendar providedCal, Long maxLen)
      throws IOException {
    Calendar cal = providedCal == null ? Calendar.getInstance() : providedCal;
    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
    sdf.setTimeZone(cal.getTimeZone());
    String dateString = sdf.format(val);

    encoder.writeByte('\'');
    encoder.writeAscii(dateString);
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, Context context, Time value, Calendar providedCal)
      throws IOException {
    Calendar cal = providedCal == null ? Calendar.getInstance() : providedCal;
    cal.setTime(value);
    cal.set(Calendar.DAY_OF_MONTH, 1);
    if (cal.get(Calendar.MILLISECOND) > 0) {
      encoder.writeByte((byte) 12);
      encoder.writeByte((byte) 0);
      encoder.writeInt(0);
      encoder.writeByte((byte) cal.get(Calendar.HOUR_OF_DAY));
      encoder.writeByte((byte) cal.get(Calendar.MINUTE));
      encoder.writeByte((byte) cal.get(Calendar.SECOND));
      encoder.writeInt(cal.get(Calendar.MILLISECOND) * 1000);
    } else {
      encoder.writeByte((byte) 8); // length
      encoder.writeByte((byte) 0);
      encoder.writeInt(0);
      encoder.writeByte((byte) cal.get(Calendar.HOUR_OF_DAY));
      encoder.writeByte((byte) cal.get(Calendar.MINUTE));
      encoder.writeByte((byte) cal.get(Calendar.SECOND));
    }
  }

  public DataType getBinaryEncodeType() {
    return DataType.TIME;
  }
}
