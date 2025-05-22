// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import static org.mariadb.jdbc.client.result.Result.NULL_LENGTH;

import java.io.IOException;
import java.sql.SQLDataException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.column.TimestampColumn;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** OffsetDateTime codec */
public class OffsetDateTimeCodec implements Codec<OffsetDateTime> {

  /** default instance */
  public static final OffsetDateTimeCodec INSTANCE = new OffsetDateTimeCodec();

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
    return OffsetDateTime.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && type.isAssignableFrom(OffsetDateTime.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof OffsetDateTime;
  }

  @Override
  public OffsetDateTime decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar calParam,
      final Context context)
      throws SQLDataException, IOException {

    switch (column.getType()) {
      case DATETIME:
      case TIMESTAMP:
        ZonedDateTime zdt =
            ZonedDateTimeCodec.INSTANCE.decodeText(buf, length, column, calParam, context);
        if (zdt == null) return null;
        return zdt.toOffsetDateTime();
      case STRING:
      case VARCHAR:
      case VARSTRING:
        try {
          int[] parts = LocalDateTimeCodec.parseTextTimestamp(buf, length);
          if (LocalDateTimeCodec.isZeroTimestamp(parts)) {
            length.set(NULL_LENGTH);
            return null;
          }
          return TimestampColumn.localDateTimeToZoneDateTime(
                  LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
                      .plusNanos(parts[6]),
                  calParam,
                  context)
              .toOffsetDateTime();
        } catch (Throwable e) {
          String val = buf.readString(length.get());
          try {
            return OffsetDateTime.parse(val);
          } catch (Throwable ee) {
            // eat
          }
          throw new SQLDataException(
              String.format(
                  "value '%s' (%s) cannot be decoded as OffsetDateTime", val, column.getType()));
        }

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format(
                "value of type %s cannot be decoded as OffsetDateTime", column.getType()));
    }
  }

  @Override
  public OffsetDateTime decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar calParam,
      final Context context)
      throws SQLDataException, IOException {

    switch (column.getType()) {
      case DATETIME:
      case TIMESTAMP:
        ZonedDateTime zdt =
            ZonedDateTimeCodec.INSTANCE.decodeBinary(buf, length, column, calParam, context);
        if (zdt == null) return null;
        return zdt.toOffsetDateTime();
      case STRING:
      case VARCHAR:
      case VARSTRING:
        try {
          int[] parts = LocalDateTimeCodec.parseTextTimestamp(buf, length);
          if (LocalDateTimeCodec.isZeroTimestamp(parts)) {
            length.set(NULL_LENGTH);
            return null;
          }
          return TimestampColumn.localDateTimeToZoneDateTime(
                  LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
                      .plusNanos(parts[6]),
                  calParam,
                  context)
              .toOffsetDateTime();
        } catch (Throwable e) {
          String val = buf.readString(length.get());
          try {
            return OffsetDateTime.parse(val);
          } catch (Throwable ee) {
            // eat
          }
          throw new SQLDataException(
              String.format(
                  "value '%s' (%s) cannot be decoded as OffsetDateTime", val, column.getType()));
        }

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format(
                "value of type %s cannot be decoded as OffsetDateTime", column.getType()));
    }
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object val, Calendar calParam, Long maxLen)
      throws IOException {
    OffsetDateTime zdt = (OffsetDateTime) val;
    Calendar cal = calParam == null ? context.getDefaultCalendar() : calParam;
    encoder.writeByte('\'');
    encoder.writeAscii(
        zdt.atZoneSameInstant(cal.getTimeZone().toZoneId())
            .format(
                zdt.getNano() != 0
                    ? LocalDateTimeCodec.TIMESTAMP_FORMAT
                    : LocalDateTimeCodec.TIMESTAMP_FORMAT_NO_FRACTIONAL));
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(
      Writer encoder, Context context, Object value, Calendar calParam, Long maxLength)
      throws IOException {
    OffsetDateTime zdt = (OffsetDateTime) value;
    Calendar cal = calParam == null ? context.getDefaultCalendar() : calParam;
    ZonedDateTime convertedZdt = zdt.atZoneSameInstant(cal.getTimeZone().toZoneId());
    int nano = convertedZdt.getNano();
    if (nano > 0) {
      encoder.writeByte((byte) 11);
      encoder.writeShort((short) convertedZdt.getYear());
      encoder.writeByte(convertedZdt.getMonthValue());
      encoder.writeByte(convertedZdt.getDayOfMonth());
      encoder.writeByte(convertedZdt.getHour());
      encoder.writeByte(convertedZdt.getMinute());
      encoder.writeByte(convertedZdt.getSecond());
      encoder.writeInt(nano / 1000);
    } else {
      encoder.writeByte((byte) 7);
      encoder.writeShort((short) convertedZdt.getYear());
      encoder.writeByte(convertedZdt.getMonthValue());
      encoder.writeByte(convertedZdt.getDayOfMonth());
      encoder.writeByte(convertedZdt.getHour());
      encoder.writeByte(convertedZdt.getMinute());
      encoder.writeByte(convertedZdt.getSecond());
    }
  }

  public int getBinaryEncodeType() {
    return DataType.DATETIME.get();
  }
}
