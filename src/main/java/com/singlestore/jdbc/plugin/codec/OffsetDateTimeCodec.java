// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.

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
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.EnumSet;

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
          DataType.VARCHAR,
          DataType.CHAR,
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
  public int getApproximateTextProtocolLength(Object value) throws SQLException {
    return 15;
  }

  @Override
  public OffsetDateTime decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar calParam)
      throws SQLDataException {

    switch (column.getType()) {
      case DATETIME:
      case TIMESTAMP:
        LocalDateTime localDateTime =
            LocalDateTimeCodec.INSTANCE.decodeText(buf, length, column, calParam);
        if (localDateTime == null) {
          return null;
        }
        Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
        return localDateTime.atZone(cal.getTimeZone().toZoneId()).toOffsetDateTime();
      case VARCHAR:
      case CHAR:
        String val = buf.readString(length.get());
        try {
          return OffsetDateTime.parse(val);
        } catch (Throwable e) {
          // eat
        }
        throw new SQLDataException(
            String.format(
                "value '%s' (%s) cannot be decoded as OffsetDateTime", val, column.getType()));
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
      final Calendar calParam)
      throws SQLDataException {

    switch (column.getType()) {
      case DATETIME:
      case TIMESTAMP:
        LocalDateTime localDateTime =
            LocalDateTimeCodec.INSTANCE.decodeBinary(buf, length, column, calParam);
        if (localDateTime == null) {
          return null;
        }
        Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
        return localDateTime.atZone(cal.getTimeZone().toZoneId()).toOffsetDateTime();
      case VARCHAR:
      case CHAR:
        String val = buf.readString(length.get());
        try {
          return OffsetDateTime.parse(val);
        } catch (Throwable e) {
          // eat
        }
        throw new SQLDataException(
            String.format(
                "value '%s' (%s) cannot be decoded as OffsetDateTime", val, column.getType()));

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format(
                "value of type %s cannot be decoded as OffsetDateTime", column.getType()));
    }
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object val, Calendar calParam, Long length)
      throws IOException {
    OffsetDateTime zdt = (OffsetDateTime) val;
    // When preserveInstants is enabled, emit FROM_UNIXTIME(epoch) to preserve
    // the absolute UTC instant of the OffsetDateTime regardless of the server's
    // @@session.time_zone interpretation at INSERT time. This avoids the 1-hour
    // drift that occurs with naked wall-clock literals when the JVM IANA tzdata
    // and the server session timezone disagree across DST or historical
    // timezone boundaries (e.g. 1987-1988 KDT in Asia/Seoul).
    //
    // FROM_UNIXTIME's accepted range is [0, INT32_MAX] (1970-01-01 00:00:00 UTC
    // through 2038-01-19 03:14:07 UTC). Values outside that range return NULL
    // on the server, which would silently lose data — so we fall back to the
    // wall-clock literal path for negative or post-2038 OffsetDateTime values.
    //
    // Aligns with MySQL Connector/J's preserveInstants option (default true
    // since 8.0.23) and MariaDB Connector/J's preserveInstants option.
    long epochSec = zdt.toEpochSecond();
    if (context.getConf().preserveInstants()
        && epochSec >= 0L
        && epochSec <= (long) Integer.MAX_VALUE) {
      // Build FROM_UNIXTIME(...) without String.format to avoid the regex
      // parsing and intermediate allocations in this hot path (every batched
      // OffsetDateTime parameter passes through here when
      // rewriteBatchedStatements=true).
      // TIMESTAMP/DATETIME column precision tops out at microseconds, so
      // sub-microsecond nanoseconds (1..999) are truncated server-side
      // anyway; only emit the fractional part when it is non-zero in
      // microsecond resolution to keep the literal clean.
      int micros = zdt.getNano() / 1000;
      encoder.writeAscii("FROM_UNIXTIME(");
      encoder.writeAscii(Long.toString(epochSec));
      if (micros > 0) {
        encoder.writeByte('.');
        // Manual six-digit zero-padding (micros is guaranteed < 1_000_000).
        if (micros < 100000) encoder.writeByte('0');
        if (micros < 10000) encoder.writeByte('0');
        if (micros < 1000) encoder.writeByte('0');
        if (micros < 100) encoder.writeByte('0');
        if (micros < 10) encoder.writeByte('0');
        encoder.writeAscii(Integer.toString(micros));
      }
      encoder.writeByte(')');
      return;
    }
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
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
  public void encodeBinary(Writer encoder, Object value, Calendar calParam, Long length)
      throws IOException {
    OffsetDateTime zdt = (OffsetDateTime) value;
    Calendar cal = calParam == null ? Calendar.getInstance() : calParam;
    ZonedDateTime convertedZdt = zdt.atZoneSameInstant(cal.getTimeZone().toZoneId());
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
