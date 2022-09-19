// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.plugin.Codec;

/** ZonedDateTime codec */
public class ZonedDateTimeCodec implements Codec<ZonedDateTime> {

  /** default instance */
  public static final ZonedDateTimeCodec INSTANCE = new ZonedDateTimeCodec();

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
    return ZonedDateTime.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && type.isAssignableFrom(ZonedDateTime.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof ZonedDateTime;
  }

  @Override
  public ZonedDateTime decodeText(
      ReadableByteBuf buf, int length, ColumnDecoder column, Calendar calParam)
      throws SQLDataException {
    LocalDateTime localDateTime =
        LocalDateTimeCodec.INSTANCE.decodeText(buf, length, column, calParam);
    if (localDateTime == null) return null;
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
    return localDateTime.atZone(cal.getTimeZone().toZoneId());
  }

  @Override
  public ZonedDateTime decodeBinary(
      ReadableByteBuf buf, int length, ColumnDecoder column, Calendar calParam)
      throws SQLDataException {
    LocalDateTime localDateTime =
        LocalDateTimeCodec.INSTANCE.decodeBinary(buf, length, column, calParam);
    if (localDateTime == null) return null;
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
    return localDateTime.atZone(cal.getTimeZone().toZoneId());
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object val, Calendar calParam, Long maxLen)
      throws IOException {
    ZonedDateTime zdt = (ZonedDateTime) val;
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
    encoder.writeByte('\'');
    encoder.writeAscii(
        zdt.withZoneSameInstant(cal.getTimeZone().toZoneId())
            .format(
                zdt.getNano() != 0
                    ? LocalDateTimeCodec.TIMESTAMP_FORMAT
                    : LocalDateTimeCodec.TIMESTAMP_FORMAT_NO_FRACTIONAL));
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar calParam, Long maxLength)
      throws IOException {
    ZonedDateTime zdt = (ZonedDateTime) value;
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
    ZonedDateTime convertedZdt = zdt.withZoneSameInstant(cal.getTimeZone().toZoneId());
    int nano = convertedZdt.getNano();
    if (nano > 0) {
      encoder.writeByte((byte) 11);
      encoder.writeShort((short) convertedZdt.get(ChronoField.YEAR));
      encoder.writeByte(convertedZdt.get(ChronoField.MONTH_OF_YEAR));
      encoder.writeByte(convertedZdt.get(ChronoField.DAY_OF_MONTH));
      encoder.writeByte(convertedZdt.get(ChronoField.HOUR_OF_DAY));
      encoder.writeByte(convertedZdt.get(ChronoField.MINUTE_OF_HOUR));
      encoder.writeByte(convertedZdt.get(ChronoField.SECOND_OF_MINUTE));
      encoder.writeInt(nano / 1000);
    } else {
      encoder.writeByte((byte) 7);
      encoder.writeShort((short) convertedZdt.get(ChronoField.YEAR));
      encoder.writeByte(convertedZdt.get(ChronoField.MONTH_OF_YEAR));
      encoder.writeByte(convertedZdt.get(ChronoField.DAY_OF_MONTH));
      encoder.writeByte(convertedZdt.get(ChronoField.HOUR_OF_DAY));
      encoder.writeByte(convertedZdt.get(ChronoField.MINUTE_OF_HOUR));
      encoder.writeByte(convertedZdt.get(ChronoField.SECOND_OF_MINUTE));
    }
  }

  public int getBinaryEncodeType() {
    return DataType.DATETIME.get();
  }
}
