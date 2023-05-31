// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.codec.list;

import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.context.Context;
import com.singlestore.jdbc.client.socket.PacketWriter;
import com.singlestore.jdbc.codec.Codec;
import com.singlestore.jdbc.codec.DataType;
import com.singlestore.jdbc.message.server.ColumnDefinitionPacket;
import java.io.IOException;
import java.sql.SQLDataException;
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

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && type.isAssignableFrom(OffsetDateTime.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof OffsetDateTime;
  }

  @Override
  public OffsetDateTime decodeText(
      final ReadableByteBuf buf,
      final int length,
      final ColumnDefinitionPacket column,
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
        String val = buf.readString(length);
        try {
          return OffsetDateTime.parse(val);
        } catch (Throwable e) {
          // eat
        }
        throw new SQLDataException(
            String.format(
                "value '%s' (%s) cannot be decoded as OffsetDateTime", val, column.getType()));
      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format(
                "value of type %s cannot be decoded as OffsetDateTime", column.getType()));
    }
  }

  @Override
  public OffsetDateTime decodeBinary(
      final ReadableByteBuf buf,
      final int length,
      final ColumnDefinitionPacket column,
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
        String val = buf.readString(length);
        try {
          return OffsetDateTime.parse(val);
        } catch (Throwable e) {
          // eat
        }
        throw new SQLDataException(
            String.format(
                "value '%s' (%s) cannot be decoded as OffsetDateTime", val, column.getType()));

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format(
                "value of type %s cannot be decoded as OffsetDateTime", column.getType()));
    }
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Object val, Calendar calParam, Long length)
      throws IOException {
    OffsetDateTime zdt = (OffsetDateTime) val;
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
  public void encodeBinary(PacketWriter encoder, Object value, Calendar calParam, Long length)
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
