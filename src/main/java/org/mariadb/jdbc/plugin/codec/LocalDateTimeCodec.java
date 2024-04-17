// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** LocalDateTime codec */
public class LocalDateTimeCodec implements Codec<LocalDateTime> {

  /** default instance */
  public static final LocalDateTimeCodec INSTANCE = new LocalDateTimeCodec();

  /** timestamp with fractional part formatter */
  public static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

  /** timestamp without fractional part formatter */
  public static final DateTimeFormatter TIMESTAMP_FORMAT_NO_FRACTIONAL =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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

  /**
   * Parse timestamp to date/month/year int array
   *
   * @param raw string data
   * @return date/month/year int array
   * @throws DateTimeException if wrong format
   */
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

    // fix non-leading tray for nanoseconds
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
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    ZonedDateTime zdt = ZonedDateTimeCodec.INSTANCE.decodeText(buf, length, column, cal, context);
    if (zdt == null) return null;
    return zdt.toLocalDateTime();
  }

  @Override
  @SuppressWarnings("fallthrough")
  public LocalDateTime decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    ZonedDateTime zdt = ZonedDateTimeCodec.INSTANCE.decodeBinary(buf, length, column, cal, context);
    if (zdt == null) return null;
    return zdt.toLocalDateTime();
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
  public void encodeBinary(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    LocalDateTime val = (LocalDateTime) value;
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
