// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLDataException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
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

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Date.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Date;
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Date decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    return column.decodeDateText(buf, length, cal, context);
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Date decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    return column.decodeDateBinary(buf, length, cal, context);
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object val, Calendar providedCal, Long maxLen)
      throws IOException {
    Calendar cal = providedCal == null ? context.getDefaultCalendar() : providedCal;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    sdf.setTimeZone(cal.getTimeZone());
    String dateString = sdf.format(val);

    encoder.writeByte('\'');
    encoder.writeAscii(dateString);
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(
      Writer encoder, Context context, Object value, Calendar providedCal, Long maxLength)
      throws IOException {
    if (providedCal == null) {
      Calendar cal = Calendar.getInstance();
      cal.clear();
      cal.setTimeInMillis(((java.util.Date) value).getTime());
      encoder.writeByte(4); // length
      encoder.writeShort((short) cal.get(Calendar.YEAR));
      encoder.writeByte(((cal.get(Calendar.MONTH) + 1) & 0xff));
      encoder.writeByte((cal.get(Calendar.DAY_OF_MONTH) & 0xff));
    } else {
      synchronized (providedCal) {
        providedCal.clear();
        providedCal.setTimeInMillis(((java.util.Date) value).getTime());
        encoder.writeByte(4); // length
        encoder.writeShort((short) providedCal.get(Calendar.YEAR));
        encoder.writeByte(((providedCal.get(Calendar.MONTH) + 1) & 0xff));
        encoder.writeByte((providedCal.get(Calendar.DAY_OF_MONTH) & 0xff));
      }
    }
  }

  public int getBinaryEncodeType() {
    return DataType.DATE.get();
  }
}
