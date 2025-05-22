// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.time.*;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** Instant codec */
public class InstantCodec implements Codec<Instant> {

  /** default instance */
  public static final InstantCodec INSTANCE = new InstantCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.DATETIME,
          DataType.DATE,
          DataType.YEAR,
          DataType.TIMESTAMP,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING,
          DataType.TIME,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Instant.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Instant.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Instant;
  }

  @Override
  public Instant decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar calParam,
      final Context context)
      throws SQLDataException, IOException {
    LocalDateTime localDateTime =
        LocalDateTimeCodec.INSTANCE.decodeText(buf, length, column, calParam, context);
    if (localDateTime == null) return null;
    return localDateTime.atZone(ZoneId.systemDefault()).toInstant();
  }

  @Override
  public Instant decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar calParam,
      final Context context)
      throws SQLDataException, IOException {
    LocalDateTime localDateTime =
        LocalDateTimeCodec.INSTANCE.decodeBinary(buf, length, column, calParam, context);
    if (localDateTime == null) return null;
    return localDateTime.atZone(ZoneId.systemDefault()).toInstant();
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object val, Calendar calParam, Long maxLen)
      throws IOException {
    Instant instant = (Instant) val;

    encoder.writeByte('\'');
    if (calParam == null && "UTC".equals(ZoneId.systemDefault().getId())) {
      // reusing ISO6801 format, replacing T by space and removing Z
      encoder.writeAscii(instant.toString().replace('T', ' '));
      encoder.pos(encoder.pos() - 1); // remove 'Z'
    } else {
      ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
      if (calParam != null) {
        zonedDateTime = zonedDateTime.withZoneSameInstant(calParam.getTimeZone().toZoneId());
      }
      encoder.writeAscii(
          zonedDateTime.format(
              instant.getNano() != 0
                  ? LocalDateTimeCodec.TIMESTAMP_FORMAT
                  : LocalDateTimeCodec.TIMESTAMP_FORMAT_NO_FRACTIONAL));
    }
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(
      Writer encoder, Context context, Object value, Calendar calParam, Long maxLength)
      throws IOException {
    Instant instant = (Instant) value;
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
    if (calParam != null) {
      zonedDateTime = zonedDateTime.withZoneSameInstant(calParam.getTimeZone().toZoneId());
    }

    int nano = zonedDateTime.getNano();
    if (nano > 0) {
      encoder.writeByte((byte) 11);
      encoder.writeShort((short) zonedDateTime.getYear());
      encoder.writeByte(zonedDateTime.getMonth().getValue());
      encoder.writeByte(zonedDateTime.getDayOfMonth());
      encoder.writeByte(zonedDateTime.getHour());
      encoder.writeByte(zonedDateTime.getMinute());
      encoder.writeByte(zonedDateTime.getSecond());
      encoder.writeInt(nano / 1000);
    } else {
      encoder.writeByte((byte) 7);
      encoder.writeShort((short) zonedDateTime.getYear());
      encoder.writeByte(zonedDateTime.getMonthValue());
      encoder.writeByte(zonedDateTime.getDayOfMonth());
      encoder.writeByte(zonedDateTime.getHour());
      encoder.writeByte(zonedDateTime.getMinute());
      encoder.writeByte(zonedDateTime.getSecond());
    }
  }

  public int getBinaryEncodeType() {
    return DataType.DATETIME.get();
  }
}
