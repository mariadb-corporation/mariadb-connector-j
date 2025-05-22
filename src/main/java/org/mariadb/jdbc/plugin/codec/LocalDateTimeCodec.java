// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
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

  public static int[] parseTextTimestamp(ReadableByteBuf buf, MutableInt length)
      throws IOException {
    int pos = buf.pos();
    buf.ensureAvailable(length.get());
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
  @SuppressWarnings("fallthrough")
  public LocalDateTime decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException, IOException {
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
      throws SQLDataException, IOException {
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
