// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

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
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.EnumSet;

public class LocalDateTimeCodec implements Codec<LocalDateTime> {

  public static final LocalDateTimeCodec INSTANCE = new LocalDateTimeCodec();
  public static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
  public static final DateTimeFormatter TIMESTAMP_FORMAT_NO_FRACTIONAL =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  public static final DateTimeFormatter SINGLESTORE_LOCAL_DATE_TIME;
  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.VARCHAR,
          DataType.CHAR,
          DataType.TIME,
          DataType.YEAR,
          DataType.DATE,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  static {
    SINGLESTORE_LOCAL_DATE_TIME =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .toFormatter();
  }

  public static int[] parseTimestamp(String raw) throws DateTimeException {
    int nanoLen = -1;
    int[] timestampsPart = new int[] {0, 0, 0, 0, 0, 0, 0};
    int partIdx = 0;
    for (int idx = 0; idx < raw.length(); idx++) {
      char b = raw.charAt(idx);
      if (b == '-' || b == ' ' || b == ':') {
        partIdx++;
        continue;
      }
      if (b == '.') {
        partIdx++;
        nanoLen = 0;
        continue;
      }
      if (nanoLen >= 0) nanoLen++;
      timestampsPart[partIdx] = timestampsPart[partIdx] * 10 + b - 48;
    }
    if (partIdx < 2) throw new DateTimeException("Wrong timestamp format");
    if (timestampsPart[0] == 0 && timestampsPart[1] == 0 && timestampsPart[2] == 0) {
      if (timestampsPart[3] == 0
          && timestampsPart[4] == 0
          && timestampsPart[5] == 0
          && timestampsPart[6] == 0) return null;
      timestampsPart[1] = 1;
      timestampsPart[2] = 1;
    }

    // fix non leading tray for nanoseconds
    if (nanoLen >= 0) {
      for (int begin = 0; begin < 6 - nanoLen; begin++) {
        timestampsPart[6] = timestampsPart[6] * 10;
      }
      timestampsPart[6] = timestampsPart[6] * 1000;
    }
    return timestampsPart;
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
  @SuppressWarnings("fallthrough")
  public LocalDateTime decodeText(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
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
              String.format("Data type %s cannot be decoded as LocalDateTime", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case CHAR:
      case VARCHAR:
        String val = buf.readString(length.get());
        try {
          parts = parseTimestamp(val);
          if (parts == null) return null;
          return LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
              .plusNanos(parts[6]);
        } catch (DateTimeException dte) {
          throw new SQLDataException(
              String.format(
                  "value '%s' (%s) cannot be decoded as LocalDateTime", val, column.getType()));
        }

      case DATE:
        parts = LocalDateCodec.parseDate(buf, length);
        if (parts == null) return null;
        return LocalDateTime.of(parts[0], parts[1], parts[2], 0, 0, 0);

      case DATETIME:
      case TIMESTAMP:
        parts = parseTimestamp(buf.readAscii(length.get()));
        if (parts == null) return null;
        return LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
            .plusNanos(parts[6]);

      case TIME:
        parts = LocalTimeCodec.parseTime(buf, length, column);
        if (parts[0] == -1) {
          return LocalDateTime.of(1970, 1, 1, 0, 0)
              .minusHours(parts[1] % 24)
              .minusMinutes(parts[2])
              .minusSeconds(parts[3])
              .minusNanos(parts[4]);
        }
        return LocalDateTime.of(1970, 1, 1, parts[1] % 24, parts[2], parts[3]).plusNanos(parts[4]);

      case YEAR:
        int year = Integer.parseInt(buf.readAscii(length.get()));
        if (column.getColumnLength() <= 2) year += year >= 70 ? 1900 : 2000;
        return LocalDateTime.of(year, 1, 1, 0, 0);

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as LocalDateTime", column.getType()));
    }
  }

  @Override
  @SuppressWarnings("fallthrough")
  public LocalDateTime decodeBinary(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
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
              .minusNanos(microseconds * 1000);
        }
        break;

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length.get());
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as LocalDateTime", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case CHAR:
      case VARCHAR:
        String val = buf.readString(length.get());
        try {
          int[] parts = parseTimestamp(val);
          if (parts == null) return null;
          return LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
              .plusNanos(parts[6]);
        } catch (DateTimeException dte) {
          throw new SQLDataException(
              String.format(
                  "value '%s' (%s) cannot be decoded as LocalDateTime", val, column.getType()));
        }

      case DATE:
      case TIMESTAMP:
      case DATETIME:
        if (length.get() == 0) return null;
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
        break;

      case YEAR:
        year = buf.readUnsignedShort();
        if (column.getColumnLength() <= 2) year += year >= 70 ? 1900 : 2000;
        break;
      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as LocalDateTime", column.getType()));
    }

    return LocalDateTime.of(year, month, (int) dayOfMonth, hour, minutes, seconds)
        .plusNanos(microseconds * 1000);
  }

  @Override
  public void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    LocalDateTime val = (LocalDateTime) value;
    encoder.writeByte('\'');
    encoder.writeAscii(
        val.format(val.getNano() != 0 ? TIMESTAMP_FORMAT : TIMESTAMP_FORMAT_NO_FRACTIONAL));
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    LocalDateTime val = (LocalDateTime) value;
    int nano = val.getNano();
    if (nano > 0) {
      encoder.writeByte((byte) 11);
      encoder.writeShort((short) val.get(ChronoField.YEAR));
      encoder.writeByte(val.get(ChronoField.MONTH_OF_YEAR));
      encoder.writeByte(val.get(ChronoField.DAY_OF_MONTH));
      encoder.writeByte(val.get(ChronoField.HOUR_OF_DAY));
      encoder.writeByte(val.get(ChronoField.MINUTE_OF_HOUR));
      encoder.writeByte(val.get(ChronoField.SECOND_OF_MINUTE));
      encoder.writeInt(nano / 1000);
    } else {
      encoder.writeByte((byte) 7);
      encoder.writeShort((short) val.get(ChronoField.YEAR));
      encoder.writeByte(val.get(ChronoField.MONTH_OF_YEAR));
      encoder.writeByte(val.get(ChronoField.DAY_OF_MONTH));
      encoder.writeByte(val.get(ChronoField.HOUR_OF_DAY));
      encoder.writeByte(val.get(ChronoField.MINUTE_OF_HOUR));
      encoder.writeByte(val.get(ChronoField.SECOND_OF_MINUTE));
    }
  }

  public int getBinaryEncodeType() {
    return DataType.DATETIME.get();
  }
}
