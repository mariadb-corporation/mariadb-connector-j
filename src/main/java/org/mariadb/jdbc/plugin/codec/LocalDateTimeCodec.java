// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import static org.mariadb.jdbc.client.result.Result.NULL_LENGTH;

import java.io.IOException;
import java.sql.SQLDataException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.Locale;
import java.util.TimeZone;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** LocalDateTime codec */
public class LocalDateTimeCodec implements Codec<LocalDateTime> {

  /** default instance */
  public static final LocalDateTimeCodec INSTANCE = new LocalDateTimeCodec();

  /** timestamp with fractional part formatter */
  public static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS", Locale.ROOT);

  /** timestamp without fractional part formatter */
  public static final DateTimeFormatter TIMESTAMP_FORMAT_NO_FRACTIONAL =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);

  /** formatter */
  public static final DateTimeFormatter MARIADB_LOCAL_DATE_TIME;

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING,
          DataType.TIME,
          DataType.YEAR,
          DataType.DATE,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  static {
    MARIADB_LOCAL_DATE_TIME =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .toFormatter();
  }

  public static int[] parseTextTimestamp(ReadableByteBuf buf, MutableInt length) {
    int pos = buf.pos();
    int nanoBegin = -1;
    int[] parts = new int[7];
    int partIdx = 0;

    for (int begin = 0; begin < length.get(); begin++) {
      byte b = buf.readByte();

      if (isDelimiter(b)) {
        partIdx++;
        if (b == '.') nanoBegin = begin;
        continue;
      }

      if (!isDigit(b)) {
        buf.pos(pos);
        throw new IllegalArgumentException("Invalid character in timestamp");
      }

      parts[partIdx] = parts[partIdx] * 10 + (b - '0');
    }

    // Adjust nanoseconds precision
    if (nanoBegin > 0) {
      adjustNanoPrecision(parts, length.get() - nanoBegin - 1);
    }
    if (partIdx < 2) {
      buf.pos(pos);
      throw new IllegalArgumentException("Wrong timestamp format");
    }
    return parts;
  }

  private static boolean isDelimiter(byte b) {
    return b == '-' || b == ' ' || b == ':' || b == '.';
  }

  private static boolean isDigit(byte b) {
    return b >= '0' && b <= '9';
  }

  private static void adjustNanoPrecision(int[] parts, int nanoLength) {
    for (int i = 0; i < 9 - nanoLength; i++) {
      parts[6] *= 10;
    }
  }

  public static boolean isZeroTimestamp(int[] parts) {
    for (int part : parts) {
      if (part != 0) return false;
    }
    return true;
  }

  public String className() {
    return LocalDateTime.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && type.isAssignableFrom(LocalDateTime.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof LocalDateTime;
  }

  @Override
  public LocalDateTime decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    switch (column.getType()) {
      case TIMESTAMP:
      case DATETIME:
        int[] parts = parseTextTimestamp(buf, length);
        if (isZeroTimestamp(parts)) {
          length.set(NULL_LENGTH);
          return null;
        }
        return fromConnectionTimeZone(
            LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6]),
            cal,
            context);

      default:
        ZonedDateTime zdt =
            ZonedDateTimeCodec.INSTANCE.decodeText(buf, length, column, cal, context);
        return zdt == null ? null : zdt.toLocalDateTime();
    }
  }

  @Override
  public LocalDateTime decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    switch (column.getType()) {
      case TIMESTAMP:
      case DATETIME:
        if (length.get() == 0) {
          length.set(NULL_LENGTH);
          return null;
        }
        int year = buf.readUnsignedShort();
        int month = buf.readByte();
        int dayOfMonth = buf.readByte();
        int hour = 0;
        int minutes = 0;
        int seconds = 0;
        long microseconds = 0;
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
        return fromConnectionTimeZone(
            LocalDateTime.of(
                year, month, dayOfMonth, hour, minutes, seconds, (int) (microseconds * 1000)),
            cal,
            context);

      default:
        ZonedDateTime zdt =
            ZonedDateTimeCodec.INSTANCE.decodeBinary(buf, length, column, cal, context);
        return zdt == null ? null : zdt.toLocalDateTime();
    }
  }

  /**
   * Apply timezone handling when DECODING a DATETIME/TIMESTAMP wall-clock value as a LocalDateTime.
   * By default the value is returned verbatim (LocalDateTime is zoneless). Only when {@code
   * pureLocalDateTime=false} and {@code preserveInstants} is set (no Calendar) is the instant
   * converted from the connection time zone to the JVM default. A Calendar has no effect on a
   * zoneless LocalDateTime.
   */
  private static LocalDateTime fromConnectionTimeZone(
      final LocalDateTime ldt, final Calendar cal, final Context context) {
    if (cal == null
        && context.getConf().preserveInstants()
        && !context.getConf().pureLocalDateTime()) {
      return ldt.atZone(context.getConnectionTimeZone().toZoneId())
          .withZoneSameInstant(TimeZone.getDefault().toZoneId())
          .toLocalDateTime();
    }
    return ldt;
  }

  /**
   * Inverse of {@link #fromConnectionTimeZone}, applied when ENCODING a LocalDateTime. By default,
   * the value is sent verbatim. Only when {@code pureLocalDateTime=false} and {@code
   * preserveInstants} is set (no Calendar) is the value, taken as a JVM-default-zone wall-clock,
   * converted to the connection time zone before being sent (so it round-trips to the same
   * instant).
   */
  private static LocalDateTime toConnectionTimeZone(
      final LocalDateTime ldt, final Calendar cal, final Context context) {
    if (cal == null
        && context.getConf().preserveInstants()
        && !context.getConf().pureLocalDateTime()) {
      return ldt.atZone(TimeZone.getDefault().toZoneId())
          .withZoneSameInstant(context.getConnectionTimeZone().toZoneId())
          .toLocalDateTime();
    }
    return ldt;
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, LocalDateTime value, Calendar cal, Long maxLen)
      throws IOException {
    LocalDateTime val = toConnectionTimeZone(value, cal, context);
    encoder.writeByte('\'');
    encoder.writeAscii(
        val.format(val.getNano() != 0 ? TIMESTAMP_FORMAT : TIMESTAMP_FORMAT_NO_FRACTIONAL));
    encoder.writeByte('\'');
  }

  @Override
  public int getApproximateTextProtocolLength(LocalDateTime value, Long length) {
    return value.getNano() > 0 ? 28 : 21;
  }

  @Override
  public void encodeBinary(
      final Writer encoder,
      final Context context,
      final LocalDateTime value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    LocalDateTime val = toConnectionTimeZone(value, cal, context);
    int nano = val.getNano();
    if (nano > 0) {
      encoder.writeByte((byte) 11);
      encoder.writeShort((short) val.getYear());
      encoder.writeByte(val.getMonthValue());
      encoder.writeByte(val.getDayOfMonth());
      encoder.writeByte(val.getHour());
      encoder.writeByte(val.getMinute());
      encoder.writeByte(val.getSecond());
      encoder.writeInt(nano / 1000);
    } else {
      encoder.writeByte((byte) 7);
      encoder.writeShort((short) val.getYear());
      encoder.writeByte(val.getMonthValue());
      encoder.writeByte(val.getDayOfMonth());
      encoder.writeByte(val.getHour());
      encoder.writeByte(val.getMinute());
      encoder.writeByte(val.getSecond());
    }
  }

  public int getBinaryEncodeType() {
    return DataType.DATETIME.get();
  }
}
