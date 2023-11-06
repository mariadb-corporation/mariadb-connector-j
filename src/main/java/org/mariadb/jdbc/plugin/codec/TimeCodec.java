// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** Time codec */
public class TimeCodec implements Codec<Time> {

  /** default instance */
  public static final TimeCodec INSTANCE = new TimeCodec();

  /** reference local date */
  public static final LocalDate EPOCH_DATE = LocalDate.of(1970, 1, 1);

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TIME,
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Time.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && type.isAssignableFrom(Time.class)
        && !type.equals(java.util.Date.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Time;
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Time decodeText(ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    return column.decodeTimeText(buf, length, cal);
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Time decodeBinary(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar calParam)
      throws SQLDataException {
    return column.decodeTimeBinary(buf, length, calParam);
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object val, Calendar providedCal, Long maxLen)
      throws IOException {
    Calendar cal = providedCal == null ? Calendar.getInstance() : providedCal;
    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
    sdf.setTimeZone(cal.getTimeZone());
    String dateString = sdf.format(val);

    encoder.writeByte('\'');
    encoder.writeAscii(dateString);
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar providedCal, Long maxLength)
      throws IOException {
    if (providedCal == null) {
      Calendar cal = Calendar.getInstance();
      cal.clear();
      cal.setTime((Time) value);
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
    } else {
      synchronized (providedCal) {
        providedCal.clear();
        providedCal.setTime((Time) value);
        providedCal.set(Calendar.DAY_OF_MONTH, 1);
        if (providedCal.get(Calendar.MILLISECOND) > 0) {
          encoder.writeByte((byte) 12);
          encoder.writeByte((byte) 0);
          encoder.writeInt(0);
          encoder.writeByte((byte) providedCal.get(Calendar.HOUR_OF_DAY));
          encoder.writeByte((byte) providedCal.get(Calendar.MINUTE));
          encoder.writeByte((byte) providedCal.get(Calendar.SECOND));
          encoder.writeInt(providedCal.get(Calendar.MILLISECOND) * 1000);
        } else {
          encoder.writeByte((byte) 8); // length
          encoder.writeByte((byte) 0);
          encoder.writeInt(0);
          encoder.writeByte((byte) providedCal.get(Calendar.HOUR_OF_DAY));
          encoder.writeByte((byte) providedCal.get(Calendar.MINUTE));
          encoder.writeByte((byte) providedCal.get(Calendar.SECOND));
        }
      }
    }
  }

  public int getBinaryEncodeType() {
    return DataType.TIME.get();
  }
}
