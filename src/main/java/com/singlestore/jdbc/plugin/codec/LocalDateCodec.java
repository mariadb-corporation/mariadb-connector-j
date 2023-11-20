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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.EnumSet;

public class LocalDateCodec implements Codec<LocalDate> {

  public static final LocalDateCodec INSTANCE = new LocalDateCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.DATE,
          DataType.NEWDATE,
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.YEAR,
          DataType.VARCHAR,
          DataType.CHAR,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public static int[] parseDate(ReadableByteBuf buf, MutableInt length) {
    int[] datePart = new int[] {0, 0, 0};
    int partIdx = 0;
    int idx = 0;

    while (idx++ < length.get()) {
      byte b = buf.readByte();
      if (b == '-') {
        partIdx++;
        continue;
      }
      datePart[partIdx] = datePart[partIdx] * 10 + b - 48;
    }

    if (datePart[0] == 0 && datePart[1] == 0 && datePart[2] == 0) {
      return null;
    }
    return datePart;
  }

  public String className() {
    return LocalDate.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(LocalDate.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof LocalDate;
  }

  @Override
  @SuppressWarnings("fallthrough")
  public LocalDate decodeText(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {

    int[] parts;
    switch (column.getType()) {
      case YEAR:
        short y = (short) buf.atoll(length.get());

        if (length.get() == 2 && column.getColumnLength() == 2) {
          // YEAR(2) - deprecated
          if (y <= 69) {
            y += 2000;
          } else {
            y += 1900;
          }
        }

        return LocalDate.of(y, 1, 1);
      case NEWDATE:
      case DATE:
        parts = parseDate(buf, length);
        break;

      case TIMESTAMP:
      case DATETIME:
        parts = LocalDateTimeCodec.parseTimestamp(buf.readAscii(length.get()));
        break;

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length.get());
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Date", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case VARCHAR:
      case CHAR:
        String val = buf.readString(length.get());
        String[] stDatePart = val.split("-| ");
        if (stDatePart.length < 3) {
          throw new SQLDataException(
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

        try {
          int year = Integer.parseInt(stDatePart[0]);
          int month = Integer.parseInt(stDatePart[1]);
          int dayOfMonth = Integer.parseInt(stDatePart[2]);
          if (year == 0 && month == 0 && dayOfMonth == 0) return null;
          return LocalDate.of(year, month, dayOfMonth);
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Date", column.getType()));
    }
    if (parts == null) return null;
    return LocalDate.of(parts[0], parts[1], parts[2]);
  }

  @Override
  @SuppressWarnings("fallthrough")
  public LocalDate decodeBinary(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {

    int year;
    int month = 1;
    int dayOfMonth = 1;

    switch (column.getType()) {
      case TIMESTAMP:
      case DATETIME:
        if (length.get() == 0) return null;
        year = buf.readUnsignedShort();
        month = buf.readByte();
        dayOfMonth = buf.readByte();

        if (length.get() > 4) {
          buf.skip(length.get() - 4);
        }
        return LocalDate.of(year, month, dayOfMonth);

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length.get());
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Date", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case CHAR:
      case VARCHAR:
        String val = buf.readString(length.get());
        String[] stDatePart = val.split("-| ");
        if (stDatePart.length < 3) {
          throw new SQLDataException(
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

        try {
          year = Integer.parseInt(stDatePart[0]);
          month = Integer.parseInt(stDatePart[1]);
          dayOfMonth = Integer.parseInt(stDatePart[2]);
          if (year == 0 && month == 0 && dayOfMonth == 0) return null;
          return LocalDate.of(year, month, dayOfMonth);
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

      case DATE:
      case YEAR:
        if (length.get() == 0) return null;
        year = buf.readUnsignedShort();

        if (column.getColumnLength() == 2) {
          // YEAR(2) - deprecated
          if (year <= 69) {
            year += 2000;
          } else {
            year += 1900;
          }
        }

        if (length.get() >= 4) {
          month = buf.readByte();
          dayOfMonth = buf.readByte();
        }
        return LocalDate.of(year, month, dayOfMonth);

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Date", column.getType()));
    }
  }

  @Override
  public void encodeText(Writer encoder, Context context, Object val, Calendar cal, Long maxLen)
      throws IOException {
    encoder.writeByte('\'');
    encoder.writeAscii(((LocalDate) val).format(DateTimeFormatter.ISO_LOCAL_DATE));
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar providedCal, Long maxLength)
      throws IOException {
    LocalDate val = (LocalDate) value;
    encoder.writeByte(7); // length
    encoder.writeShort((short) val.get(ChronoField.YEAR));
    encoder.writeByte(val.get(ChronoField.MONTH_OF_YEAR));
    encoder.writeByte(val.get(ChronoField.DAY_OF_MONTH));
    encoder.writeBytes(new byte[] {0, 0, 0});
  }

  public int getBinaryEncodeType() {
    return DataType.DATE.get();
  }
}
