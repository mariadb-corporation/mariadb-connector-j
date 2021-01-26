/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.codec.list;

import java.io.IOException;
import java.sql.SQLDataException;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class LocalDateTimeCodec implements Codec<LocalDateTime> {

  public static final LocalDateTimeCodec INSTANCE = new LocalDateTimeCodec();
  public static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
  public static final DateTimeFormatter TIMESTAMP_FORMAT_NO_FRACTIONAL =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  public static final DateTimeFormatter MARIADB_LOCAL_DATE_TIME;
  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING,
          DataType.TIME,
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

  public static int[] parseTimestamp(String raw) {
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

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && type.isAssignableFrom(LocalDateTime.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof LocalDateTime;
  }

  @Override
  @SuppressWarnings("fallthrough")
  public LocalDateTime decodeText(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    int[] parts;
    switch (column.getType()) {
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as LocalDateTime", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case STRING:
      case VARCHAR:
      case VARSTRING:
        String val = buf.readString(length);
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
        parts = parseTimestamp(buf.readAscii(length));
        if (parts == null) return null;
        return LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
            .plusNanos(parts[6]);

      case TIME:
        parts = LocalTimeCodec.parseTime(buf, length, column);
        return LocalDateTime.of(1970, 1, 1, parts[1] % 24, parts[2], parts[3]).plusNanos(parts[4]);

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as LocalDateTime", column.getType()));
    }
  }

  @Override
  @SuppressWarnings("fallthrough")
  public LocalDateTime decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
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
        buf.skip(5); // skip negative and days
        hour = buf.readByte();
        minutes = buf.readByte();
        seconds = buf.readByte();
        if (length > 8) {
          microseconds = buf.readUnsignedInt();
        }
        break;

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as LocalDateTime", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case STRING:
      case VARCHAR:
      case VARSTRING:
        String val = buf.readString(length);
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
        year = buf.readUnsignedShort();
        month = buf.readByte();
        dayOfMonth = buf.readByte();

        if (length > 4) {
          hour = buf.readByte();
          minutes = buf.readByte();
          seconds = buf.readByte();

          if (length > 7) {
            microseconds = buf.readUnsignedInt();
          }
        }
        break;

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as LocalDateTime", column.getType()));
    }

    return LocalDateTime.of(year, month, (int) dayOfMonth, hour, minutes, seconds)
        .plusNanos(microseconds * 1000);
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    LocalDateTime val = (LocalDateTime) value;
    encoder.writeByte('\'');
    encoder.writeAscii(
        val.format(val.getNano() != 0 ? TIMESTAMP_FORMAT : TIMESTAMP_FORMAT_NO_FRACTIONAL));
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(
      PacketWriter encoder, Context context, Object value, Calendar cal, Long maxLength)
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
