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
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class DateCodec implements Codec<Date> {

  public static final DateCodec INSTANCE = new DateCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.DATE,
          DataType.NEWDATE,
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.YEAR,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING);

  public String className() {
    return Date.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Date.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Date;
  }

  @Override
  public Date decodeText(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {

    switch (column.getType()) {
      case YEAR:
        short y = (short) LongCodec.parseNotEmpty(buf, length);
        if (column.getLength() == 2) {
          // YEAR(2) - deprecated
          if (y <= 69) {
            y += 2000;
          } else {
            y += 1900;
          }
        }
        return Date.valueOf(y + "-01-01");

      case VARCHAR:
      case VARSTRING:
      case STRING:
      case DATE:
        String val = buf.readString(length);
        String[] stDatePart = val.split("-| ");
        if (stDatePart.length < 3) {
          throw new SQLDataException(
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

        try {
          int year = Integer.valueOf(stDatePart[0]);
          int month = Integer.valueOf(stDatePart[1]);
          int dayOfMonth = Integer.valueOf(stDatePart[2]);
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
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

      case TIMESTAMP:
      case DATETIME:
        Timestamp lt = TimestampCodec.INSTANCE.decodeText(buf, length, column, cal);
        return new Date(lt.getTime());

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Date", column.getType()));
    }
  }

  @Override
  public Date decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {

    switch (column.getType()) {
      case YEAR:
        int v = buf.readShort();

        if (column.getLength() == 2) {
          // YEAR(2) - deprecated
          if (v <= 69) {
            v += 2000;
          } else {
            v += 1900;
          }
        }
        return Date.valueOf(v + "-01-01");

      case VARCHAR:
      case VARSTRING:
      case STRING:
        String val = buf.readString(length);
        String[] stDatePart = val.split("-| ");
        if (stDatePart.length < 3) {
          throw new SQLDataException(
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

        try {
          int year = Integer.valueOf(stDatePart[0]);
          int month = Integer.valueOf(stDatePart[1]);
          int dayOfMonth = Integer.valueOf(stDatePart[2]);
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
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

      case DATE:
        Calendar c = cal == null ? Calendar.getInstance() : cal;
        synchronized (c) {
          c.clear();
          c.set(Calendar.YEAR, buf.readShort());
          c.set(Calendar.MONTH, buf.readByte() - 1);
          c.set(Calendar.DAY_OF_MONTH, buf.readByte());
          return new Date(c.getTimeInMillis());
        }

      case TIMESTAMP:
      case DATETIME:
        Timestamp lt = TimestampCodec.INSTANCE.decodeBinary(buf, length, column, cal);
        return new Date(lt.getTime());

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Date", column.getType()));
    }
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Date val, Calendar providedCal, Long maxLen)
      throws IOException {
    Calendar cal = providedCal == null ? Calendar.getInstance() : providedCal;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    sdf.setTimeZone(cal.getTimeZone());
    String dateString = sdf.format(val);

    encoder.writeByte('\'');
    encoder.writeAscii(dateString);
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, Context context, Date value, Calendar providedCal)
      throws IOException {
    Calendar cal = providedCal == null ? Calendar.getInstance() : providedCal;
    cal.setTimeInMillis(value.getTime());
    encoder.writeByte(4); // length
    encoder.writeShort((short) cal.get(Calendar.YEAR));
    encoder.writeByte(((cal.get(Calendar.MONTH) + 1) & 0xff));
    encoder.writeByte((cal.get(Calendar.DAY_OF_MONTH) & 0xff));
  }

  public DataType getBinaryEncodeType() {
    return DataType.DATE;
  }
}
