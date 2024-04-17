// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import static org.mariadb.jdbc.client.result.Result.NULL_LENGTH;

import java.io.IOException;
import java.sql.SQLDataException;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.TimeZone;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.column.TimestampColumn;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
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
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar calParam,
      final Context context)
      throws SQLDataException {
    int[] parts;
    switch (column.getType()) {
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length.get());
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as ZoneDateTime", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if it has a collation (this is TEXT column)

      case STRING:
      case VARCHAR:
      case VARSTRING:
        String val = buf.readString(length.get());
        try {
          parts = LocalDateTimeCodec.parseTimestamp(val);
          if (parts == null) {
            length.set(NULL_LENGTH);
            return null;
          }
          return TimestampColumn.localDateTimeToZoneDateTime(
              LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
                  .plusNanos(parts[6]),
              calParam,
              context);
        } catch (DateTimeException dte) {
          throw new SQLDataException(
              String.format(
                  "value '%s' (%s) cannot be decoded as ZoneDateTime", val, column.getType()));
        }

      case DATE:
        parts = LocalDateCodec.parseDate(buf, length);
        if (parts == null) {
          length.set(NULL_LENGTH);
          return null;
        }
        TimeZone tz = calParam == null ? TimeZone.getDefault() : calParam.getTimeZone();
        return LocalDateTime.of(parts[0], parts[1], parts[2], 0, 0, 0).atZone(tz.toZoneId());

      case DATETIME:
      case TIMESTAMP:
        parts = LocalDateTimeCodec.parseTimestamp(buf.readAscii(length.get()));
        if (parts == null) {
          length.set(NULL_LENGTH);
          return null;
        }
        LocalDateTime ldt =
            LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
                .plusNanos(parts[6]);
        return TimestampColumn.localDateTimeToZoneDateTime(ldt, calParam, context);

      case TIME:
        parts = LocalTimeCodec.parseTime(buf, length, column);
        TimeZone tzTime = calParam == null ? TimeZone.getDefault() : calParam.getTimeZone();
        if (parts[0] == -1) {
          return LocalDateTime.of(1970, 1, 1, 0, 0)
              .minusHours(parts[1] % 24)
              .minusMinutes(parts[2])
              .minusSeconds(parts[3])
              .minusNanos(parts[4])
              .atZone(tzTime.toZoneId());
        }
        return LocalDateTime.of(1970, 1, 1, parts[1] % 24, parts[2], parts[3])
            .plusNanos(parts[4])
            .atZone(tzTime.toZoneId());

      case YEAR:
        int year = Integer.parseInt(buf.readAscii(length.get()));
        if (column.getColumnLength() <= 2) year += year >= 70 ? 1900 : 2000;
        TimeZone tzYear = calParam == null ? TimeZone.getDefault() : calParam.getTimeZone();
        return LocalDateTime.of(year, 1, 1, 0, 0).atZone(tzYear.toZoneId());

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as ZoneDateTime", column.getType()));
    }
  }

  @Override
  public ZonedDateTime decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar calParam,
      final Context context)
      throws SQLDataException {
    int year = 1970;
    int month = 1;
    long dayOfMonth = 1;
    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;

    switch (column.getType()) {
      case TIME:
        TimeZone tzTime = calParam == null ? TimeZone.getDefault() : calParam.getTimeZone();
        if (length.get() > 0) {
          // specific case for TIME, to handle value not in 00:00:00-23:59:59
          boolean negate = buf.readByte() == 1;
          int day = buf.readInt();
          hour = buf.readByte();
          minutes = buf.readByte();
          seconds = buf.readByte();
          if (length.get() > 8) {
            microseconds = buf.readUnsignedInt();
          }
          if (negate) {
            return LocalDateTime.of(1970, 1, 1, 0, 0)
                .minusDays(day)
                .minusHours(hour)
                .minusMinutes(minutes)
                .minusSeconds(seconds)
                .minusNanos(microseconds * 1000)
                .atZone(tzTime.toZoneId());
          }
        }
        return LocalDateTime.of(year, month, (int) dayOfMonth, hour, minutes, seconds)
            .plusNanos(microseconds * 1000)
            .atZone(tzTime.toZoneId());

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length.get());
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as ZoneDateTime", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if it has a collation (this is TEXT column)

      case STRING:
      case VARCHAR:
      case VARSTRING:
        String val = buf.readString(length.get());
        try {
          int[] parts = LocalDateTimeCodec.parseTimestamp(val);
          if (parts == null) {
            length.set(NULL_LENGTH);
            return null;
          }
          return TimestampColumn.localDateTimeToZoneDateTime(
              LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
                  .plusNanos(parts[6]),
              calParam,
              context);
        } catch (DateTimeException dte) {
          throw new SQLDataException(
              String.format(
                  "value '%s' (%s) cannot be decoded as ZoneDateTime", val, column.getType()));
        }

      case DATE:
        if (length.get() == 0) {
          length.set(NULL_LENGTH);
          return null;
        }
        year = buf.readUnsignedShort();
        month = buf.readByte();
        dayOfMonth = buf.readByte();

        // xpand workaround https://jira.mariadb.org/browse/XPT-274
        if (year == 0 && month == 0 && dayOfMonth == 0) {
          length.set(NULL_LENGTH);
          return null;
        }
        TimeZone tz = calParam == null ? TimeZone.getDefault() : calParam.getTimeZone();
        return LocalDateTime.of(year, month, (int) dayOfMonth, 0, 0, 0).atZone(tz.toZoneId());

      case TIMESTAMP:
      case DATETIME:
        if (length.get() == 0) {
          length.set(NULL_LENGTH);
          return null;
        }
        year = buf.readUnsignedShort();
        month = buf.readByte();
        dayOfMonth = buf.readByte();

        if (length.get() > 4) {
          hour = buf.readByte();
          minutes = buf.readByte();
          seconds = buf.readByte();

          if (length.get() > 7) {
            microseconds = buf.readUnsignedInt();
          }
        }

        // xpand workaround https://jira.mariadb.org/browse/XPT-274
        if (year == 0
            && month == 0
            && dayOfMonth == 0
            && hour == 0
            && minutes == 0
            && seconds == 0) {
          length.set(NULL_LENGTH);
          return null;
        }

        LocalDateTime ldt =
            LocalDateTime.of(year, month, (int) dayOfMonth, hour, minutes, seconds)
                .plusNanos(microseconds * 1000);
        return TimestampColumn.localDateTimeToZoneDateTime(ldt, calParam, context);

      case YEAR:
        year = buf.readUnsignedShort();
        if (column.getColumnLength() <= 2) year += year >= 70 ? 1900 : 2000;
        TimeZone tzYear = calParam == null ? TimeZone.getDefault() : calParam.getTimeZone();
        return LocalDateTime.of(year, 1, 1, 0, 0).atZone(tzYear.toZoneId());

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as LocalDateTime", column.getType()));
    }
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object val, Calendar calParam, Long maxLen)
      throws IOException {
    ZonedDateTime zdt = (ZonedDateTime) val;
    Calendar cal = calParam == null ? context.getDefaultCalendar() : calParam;
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
  public void encodeBinary(
      Writer encoder, Context context, Object value, Calendar calParam, Long maxLength)
      throws IOException {
    ZonedDateTime zdt = (ZonedDateTime) value;
    Calendar cal = calParam == null ? context.getDefaultCalendar() : calParam;
    ZonedDateTime convertedZdt = zdt.withZoneSameInstant(cal.getTimeZone().toZoneId());
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
