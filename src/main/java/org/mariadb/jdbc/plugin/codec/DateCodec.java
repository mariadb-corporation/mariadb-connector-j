// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.Column;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.plugin.Codec;

/** Date codec */
public class DateCodec implements Codec<Date> {

  /** default instance */
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
          DataType.STRING,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Date.class.getName();
  }

  public boolean canDecode(Column column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Date.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Date || java.util.Date.class.equals(value.getClass());
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Date decodeText(ReadableByteBuf buf, int length, Column column, Calendar cal)
      throws SQLDataException {

    switch (column.getType()) {
      case YEAR:
        short y = (short) buf.atoll(length);
        if (column.getColumnLength() == 2) {
          // YEAR(2) - deprecated
          if (y <= 69) {
            y += 2000;
          } else {
            y += 1900;
          }
        }
        return Date.valueOf(y + "-01-01");

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Date", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if it has a collation (this is TEXT column)

      case VARCHAR:
      case VARSTRING:
      case STRING:
      case DATE:
        String val = buf.readString(length);
        if ("0000-00-00".equals(val)) return null;
        String[] stDatePart = val.split("[- ]");
        if (stDatePart.length < 3) {
          throw new SQLDataException(
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

        return getDate(column, cal, val, stDatePart);

      case TIMESTAMP:
      case DATETIME:
        Timestamp lt = TimestampCodec.INSTANCE.decodeText(buf, length, column, cal);
        String st = lt.toString();
        return Date.valueOf(st.substring(0, 10));

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Date", column.getType()));
    }
  }

  public static Date getDate(Column column, Calendar cal, String val, String[] stDatePart)
      throws SQLDataException {
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
          String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
    }
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Date decodeBinary(ReadableByteBuf buf, int length, Column column, Calendar cal)
      throws SQLDataException {

    switch (column.getType()) {
      case YEAR:
        int v = buf.readShort();

        if (column.getColumnLength() == 2) {
          // YEAR(2) - deprecated
          if (v <= 69) {
            v += 2000;
          } else {
            v += 1900;
          }
        }
        return Date.valueOf(v + "-01-01");

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Date", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if it has a collation (this is TEXT column)

      case VARCHAR:
      case VARSTRING:
      case STRING:
        String val = buf.readString(length);
        String[] stDatePart = val.split("[- ]");
        if (stDatePart.length < 3) {
          throw new SQLDataException(
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

        return getDate(column, cal, val, stDatePart);

      case DATE:
        if (length == 0) return null;
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
        if (lt == null) return null;
        String st = lt.toString();
        return Date.valueOf(st.substring(0, 10));

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Date", column.getType()));
    }
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object val, Calendar providedCal, Long maxLen)
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
  public void encodeBinary(Writer encoder, Object value, Calendar providedCal, Long maxLength)
      throws IOException {
    Calendar cal = providedCal == null ? Calendar.getInstance() : providedCal;
    cal.setTimeInMillis(((java.util.Date) value).getTime());
    encoder.writeByte(4); // length
    encoder.writeShort((short) cal.get(Calendar.YEAR));
    encoder.writeByte(((cal.get(Calendar.MONTH) + 1) & 0xff));
    encoder.writeByte((cal.get(Calendar.DAY_OF_MONTH) & 0xff));
  }

  public int getBinaryEncodeType() {
    return DataType.DATE.get();
  }
}
