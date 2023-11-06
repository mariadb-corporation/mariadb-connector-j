// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import static org.mariadb.jdbc.client.result.Result.NULL_LENGTH;

import java.io.IOException;
import java.sql.SQLDataException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** LocalDate codec */
public class LocalDateCodec implements Codec<LocalDate> {

  /** default instance */
  public static final LocalDateCodec INSTANCE = new LocalDateCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.DATE,
          DataType.NEWDATE,
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.YEAR,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  /**
   * Parse text encoded Date
   *
   * @param buf packet buffer
   * @param length data length
   * @return date/month/year array
   */
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
      length.set(NULL_LENGTH);
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
        short y = (short) buf.atoull(length.get());

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
        // BLOB is considered as String if it has a collation (this is TEXT column)

      case VARSTRING:
      case VARCHAR:
      case STRING:
        String val = buf.readString(length.get());
        String[] stDatePart = val.split("[- ]");
        if (stDatePart.length < 3) {
          throw new SQLDataException(
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

        try {
          int year = Integer.parseInt(stDatePart[0]);
          int month = Integer.parseInt(stDatePart[1]);
          int dayOfMonth = Integer.parseInt(stDatePart[2]);
          if (year == 0 && month == 0 && dayOfMonth == 0) {
            length.set(NULL_LENGTH);
            return null;
          }
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
    if (parts == null) {
      length.set(NULL_LENGTH);
      return null;
    }
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
        if (length.get() == 0) {
          length.set(NULL_LENGTH);
          return null;
        }
        year = buf.readUnsignedShort();
        month = buf.readByte();
        dayOfMonth = buf.readByte();

        if (length.get() > 4) {
          buf.skip(length.get() - 4);
        }

        // xpand workaround https://jira.mariadb.org/browse/XPT-274
        if (year == 0 && month == 0 && dayOfMonth == 0) {
          length.set(NULL_LENGTH);
          return null;
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
        // BLOB is considered as String if it has a collation (this is TEXT column)

      case STRING:
      case VARCHAR:
      case VARSTRING:
        String val = buf.readString(length.get());
        String[] stDatePart = val.split("[- ]");
        if (stDatePart.length < 3) {
          throw new SQLDataException(
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

        try {
          year = Integer.parseInt(stDatePart[0]);
          month = Integer.parseInt(stDatePart[1]);
          dayOfMonth = Integer.parseInt(stDatePart[2]);
          if (year == 0 && month == 0 && dayOfMonth == 0) {
            length.set(NULL_LENGTH);
            return null;
          }
          return LocalDate.of(year, month, dayOfMonth);
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(
              String.format("value '%s' (%s) cannot be decoded as Date", val, column.getType()));
        }

      case DATE:
      case YEAR:
        if (length.get() == 0) {
          length.set(NULL_LENGTH);
          return null;
        }
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

        // xpand workaround https://jira.mariadb.org/browse/XPT-274
        if (year == 0 && month == 0 && dayOfMonth == 0) {
          length.set(NULL_LENGTH);
          return null;
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
    encoder.writeShort((short) val.getYear());
    encoder.writeByte(val.getMonthValue());
    encoder.writeByte(val.getDayOfMonth());
    encoder.writeBytes(new byte[] {0, 0, 0});
  }

  public int getBinaryEncodeType() {
    return DataType.DATE.get();
  }
}
