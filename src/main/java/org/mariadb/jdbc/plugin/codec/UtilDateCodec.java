// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** java.util.Date codec */
public class UtilDateCodec implements Codec<java.util.Date> {

  /** default instance */
  public static final UtilDateCodec INSTANCE = new UtilDateCodec();

  protected static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.DATE,
          DataType.NEWDATE,
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.YEAR,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING,
          DataType.TIME,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return java.util.Date.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && type.isAssignableFrom(java.util.Date.class);
  }

  public boolean canEncode(Object value) {
    return java.util.Date.class.equals(value.getClass());
  }

  /**
   * Get microseconds from a java.util.Date value.
   *
   * @param val date value
   * @return microseconds
   */
  protected static int getMicroseconds(java.util.Date val) {
    if (val instanceof Timestamp) {
      return ((Timestamp) val).getNanos() / 1000;
    }
    return (int) ((val.getTime() % 1000) * 1000);
  }

  @Override
  @SuppressWarnings("fallthrough")
  public java.util.Date decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    Timestamp ts = column.decodeTimestampText(buf, length, cal, context);
    return ts == null ? null : new java.util.Date(ts.getTime());
  }

  @Override
  @SuppressWarnings("fallthrough")
  public java.util.Date decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    Timestamp ts = column.decodeTimestampBinary(buf, length, cal, context);
    return ts == null ? null : new java.util.Date(ts.getTime());
  }

  /**
   * Shared text encoding for date/timestamp values.
   *
   * @param encoder writer
   * @param context connection context
   * @param val date value
   * @param providedCal calendar
   * @param maxLen max length
   * @throws IOException if socket error
   */
  protected static void encodeTextDate(
      Writer encoder, Context context, java.util.Date val, Calendar providedCal, Long maxLen)
      throws IOException {
    Calendar cal = providedCal == null ? context.getDefaultCalendar() : providedCal;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    sdf.setTimeZone(cal.getTimeZone());
    String dateString = sdf.format(val);

    encoder.writeByte('\'');
    encoder.writeAscii(dateString);
    int microseconds = getMicroseconds(val);

    if (microseconds > 0) {
      if (microseconds % 1000 == 0) {
        encoder.writeAscii("." + Integer.toString(microseconds / 1000 + 1000).substring(1));
      } else {
        encoder.writeAscii("." + Integer.toString(microseconds + 1000000).substring(1));
      }
    }

    encoder.writeByte('\'');
  }

  /**
   * Shared binary encoding for date/timestamp values.
   *
   * @param encoder writer
   * @param context connection context
   * @param value date value
   * @param providedCal calendar
   * @param maxLength max length
   * @throws IOException if socket error
   */
  protected static void encodeBinaryDate(
      Writer encoder, Context context, java.util.Date value, Calendar providedCal, Long maxLength)
      throws IOException {

    long timeInMillis = value.getTime();
    int microseconds = getMicroseconds(value);

    if (providedCal == null) {
      Calendar cal = context.getDefaultCalendar();
      cal.clear();
      cal.setTimeInMillis(timeInMillis);
      if (microseconds == 0) {
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
        encoder.writeInt(microseconds);
      }
    } else {
      synchronized (providedCal) {
        providedCal.clear();
        providedCal.setTimeInMillis(timeInMillis);
        if (microseconds == 0) {
          encoder.writeByte(7); // length
          encoder.writeShort((short) providedCal.get(Calendar.YEAR));
          encoder.writeByte((providedCal.get(Calendar.MONTH) + 1));
          encoder.writeByte(providedCal.get(Calendar.DAY_OF_MONTH));
          encoder.writeByte(providedCal.get(Calendar.HOUR_OF_DAY));
          encoder.writeByte(providedCal.get(Calendar.MINUTE));
          encoder.writeByte(providedCal.get(Calendar.SECOND));
        } else {
          encoder.writeByte(11); // length
          encoder.writeShort((short) providedCal.get(Calendar.YEAR));
          encoder.writeByte((providedCal.get(Calendar.MONTH) + 1));
          encoder.writeByte(providedCal.get(Calendar.DAY_OF_MONTH));
          encoder.writeByte(providedCal.get(Calendar.HOUR_OF_DAY));
          encoder.writeByte(providedCal.get(Calendar.MINUTE));
          encoder.writeByte(providedCal.get(Calendar.SECOND));
          encoder.writeInt(microseconds);
        }
      }
    }
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, java.util.Date val, Calendar providedCal, Long maxLen)
      throws IOException {
    encodeTextDate(encoder, context, val, providedCal, maxLen);
  }

  @Override
  public int getApproximateTextProtocolLength(java.util.Date value, Long length) {
    return getMicroseconds(value) > 0 ? 28 : 21;
  }

  @Override
  public void encodeBinary(
      Writer encoder, Context context, java.util.Date value, Calendar providedCal, Long maxLength)
      throws IOException {
    encodeBinaryDate(encoder, context, value, providedCal, maxLength);
  }

  public int getBinaryEncodeType() {
    return DataType.DATETIME.get();
  }
}
