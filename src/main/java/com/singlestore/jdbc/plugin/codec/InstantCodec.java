// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.plugin.codec;

import com.singlestore.jdbc.client.ColumnDecoder;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.util.MutableInt;
import com.singlestore.jdbc.plugin.Codec;
import java.io.IOException;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.EnumSet;

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
          DataType.VARCHAR,
          DataType.CHAR,
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
  public int getApproximateTextProtocolLength(Object value) throws SQLException {
    return canEncode(value) ? String.valueOf(value).getBytes().length : -1;
  }

  @Override
  public Instant decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar calParam)
      throws SQLDataException {
    LocalDateTime localDateTime =
        LocalDateTimeCodec.INSTANCE.decodeText(buf, length, column, calParam);
    if (localDateTime == null) {
      return null;
    }
    return localDateTime.atZone(ZoneId.systemDefault()).toInstant();
  }

  @Override
  public Instant decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar calParam)
      throws SQLDataException {
    LocalDateTime localDateTime =
        LocalDateTimeCodec.INSTANCE.decodeBinary(buf, length, column, calParam);
    if (localDateTime == null) {
      return null;
    }
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
  public void encodeBinary(Writer encoder, Object value, Calendar calParam, Long maxLength)
      throws IOException {
    Instant instant = (Instant) value;
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
    if (calParam != null) {
      zonedDateTime = zonedDateTime.withZoneSameInstant(calParam.getTimeZone().toZoneId());
    }

    int nano = zonedDateTime.getNano();
    if (nano > 0) {
      encoder.writeByte((byte) 11);
      encoder.writeShort((short) zonedDateTime.get(ChronoField.YEAR));
      encoder.writeByte(zonedDateTime.get(ChronoField.MONTH_OF_YEAR));
      encoder.writeByte(zonedDateTime.get(ChronoField.DAY_OF_MONTH));
      encoder.writeByte(zonedDateTime.get(ChronoField.HOUR_OF_DAY));
      encoder.writeByte(zonedDateTime.get(ChronoField.MINUTE_OF_HOUR));
      encoder.writeByte(zonedDateTime.get(ChronoField.SECOND_OF_MINUTE));
      encoder.writeInt(nano / 1000);
    } else {
      encoder.writeByte((byte) 7);
      encoder.writeShort((short) zonedDateTime.get(ChronoField.YEAR));
      encoder.writeByte(zonedDateTime.get(ChronoField.MONTH_OF_YEAR));
      encoder.writeByte(zonedDateTime.get(ChronoField.DAY_OF_MONTH));
      encoder.writeByte(zonedDateTime.get(ChronoField.HOUR_OF_DAY));
      encoder.writeByte(zonedDateTime.get(ChronoField.MINUTE_OF_HOUR));
      encoder.writeByte(zonedDateTime.get(ChronoField.SECOND_OF_MINUTE));
    }
  }

  public int getBinaryEncodeType() {
    return DataType.DATETIME.get();
  }
}
