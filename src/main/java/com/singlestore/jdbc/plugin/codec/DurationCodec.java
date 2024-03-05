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
import java.time.Duration;
import java.util.Calendar;
import java.util.EnumSet;

public class DurationCodec implements Codec<Duration> {

  public static final DurationCodec INSTANCE = new DurationCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TIME,
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.VARCHAR,
          DataType.CHAR,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Duration.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Duration.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Duration;
  }

  @Override
  public int getApproximateTextProtocolLength(Object value) throws SQLException {
    return canEncode(value) ? String.valueOf(value).getBytes().length : -1;
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Duration decodeText(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {

    int[] parts;
    switch (column.getType()) {
      case TIMESTAMP:
      case DATETIME:
        parts = LocalDateTimeCodec.parseTimestamp(buf.readAscii(length.get()));
        if (parts == null) return null;
        return Duration.ZERO
            .plusDays(parts[2] - 1)
            .plusHours(parts[3])
            .plusMinutes(parts[4])
            .plusSeconds(parts[5])
            .plusNanos(parts[6]);

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length.get());
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Duration", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case TIME:
      case VARCHAR:
      case CHAR:
        parts = LocalTimeCodec.parseTime(buf, length, column);
        Duration d =
            Duration.ZERO
                .plusHours(parts[1])
                .plusMinutes(parts[2])
                .plusSeconds(parts[3])
                .plusNanos(parts[4]);
        if (parts[0] == -1) return d.negated();
        return d;

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Duration", column.getType()));
    }
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Duration decodeBinary(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {

    long days = 0;
    int hours = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;
    switch (column.getType()) {
      case TIME:
        boolean negate = false;
        if (length.get() > 0) {
          negate = buf.readUnsignedByte() == 0x01;
          if (length.get() > 4) {
            days = buf.readUnsignedInt();
            if (length.get() > 7) {
              hours = buf.readByte();
              minutes = buf.readByte();
              seconds = buf.readByte();
              if (length.get() > 8) {
                microseconds = buf.readInt();
              }
            }
          }
        }

        Duration duration =
            Duration.ZERO
                .plusDays(days)
                .plusHours(hours)
                .plusMinutes(minutes)
                .plusSeconds(seconds)
                .plusNanos(microseconds * 1000);
        if (negate) return duration.negated();
        return duration;

      case TIMESTAMP:
      case DATETIME:
        if (length.get() == 0) return null;
        buf.readUnsignedShort(); // skip year
        buf.readByte(); // skip month
        days = buf.readByte();
        if (length.get() > 4) {
          hours = buf.readByte();
          minutes = buf.readByte();
          seconds = buf.readByte();

          if (length.get() > 7) {
            microseconds = buf.readUnsignedInt();
          }
        }
        return Duration.ZERO
            .plusDays(days - 1)
            .plusHours(hours)
            .plusMinutes(minutes)
            .plusSeconds(seconds)
            .plusNanos(microseconds * 1000);

      case VARCHAR:
      case CHAR:
        int[] parts = LocalTimeCodec.parseTime(buf, length, column);
        Duration d =
            Duration.ZERO
                .plusHours(parts[1])
                .plusMinutes(parts[2])
                .plusSeconds(parts[3])
                .plusNanos(parts[4]);
        if (parts[0] == -1) return d.negated();
        return d;

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Duration", column.getType()));
    }
  }

  @Override
  public void encodeText(Writer encoder, Context context, Object val, Calendar cal, Long maxLen)
      throws IOException {
    long s = ((Duration) val).getSeconds();
    long microSecond = ((Duration) val).getNano() / 1000;
    encoder.writeByte('\'');
    if (microSecond != 0) {
      encoder.writeAscii(
          String.format("%d:%02d:%02d.%06d", s / 3600, (s % 3600) / 60, (s % 60), microSecond));
    } else {
      encoder.writeAscii(String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60)));
    }
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(Writer encoder, Object val, Calendar cal, Long maxLength)
      throws IOException {
    int nano = ((Duration) val).getNano();
    if (nano > 0) {
      encoder.writeByte((byte) 12);
      encodeDuration(encoder, ((Duration) val));
      encoder.writeInt(nano / 1000);
    } else {
      encoder.writeByte((byte) 8);
      encodeDuration(encoder, ((Duration) val));
    }
  }

  private void encodeDuration(Writer encoder, Duration value) throws IOException {
    encoder.writeByte((byte) (value.isNegative() ? 1 : 0));
    encoder.writeInt((int) value.toDays());
    encoder.writeByte((byte) (value.toHours() - 24 * value.toDays()));
    encoder.writeByte((byte) (value.toMinutes() - 60 * value.toHours()));
    encoder.writeByte((byte) (value.getSeconds() - 60 * value.toMinutes()));
  }

  public int getBinaryEncodeType() {
    return DataType.TIME.get();
  }
}
