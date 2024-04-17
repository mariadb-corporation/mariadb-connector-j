// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import static org.mariadb.jdbc.client.result.Result.NULL_LENGTH;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.*;
import java.time.DateTimeException;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.plugin.codec.LocalDateTimeCodec;
import org.mariadb.jdbc.plugin.codec.LocalTimeCodec;
import org.mariadb.jdbc.util.CharsetEncodingLength;

/** Column metadata definition */
public class StringColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  /**
   * VARCHAR/STRING/VARSTRING metadata type decoder
   *
   * @param buf buffer
   * @param charset charset
   * @param length maximum data length
   * @param dataType data type. see https://mariadb.com/kb/en/result-set-packets/#field-types
   * @param decimals decimal length
   * @param flags flags. see https://mariadb.com/kb/en/result-set-packets/#field-details-flag
   * @param stringPos string offset position in buffer
   * @param extTypeName extended type name
   * @param extTypeFormat extended type format
   */
  public StringColumn(
      final ReadableByteBuf buf,
      final int charset,
      final long length,
      final DataType dataType,
      final byte decimals,
      final int flags,
      final int[] stringPos,
      final String extTypeName,
      final String extTypeFormat) {
    super(
        buf,
        charset,
        length,
        dataType,
        decimals,
        flags,
        stringPos,
        extTypeName,
        extTypeFormat,
        false);
  }

  protected StringColumn(StringColumn prev) {
    super(prev, true);
  }

  @Override
  public StringColumn useAliasAsName() {
    return new StringColumn(this);
  }

  public String defaultClassname(final Configuration conf) {
    return isBinary() ? "byte[]" : String.class.getName();
  }

  public int getColumnType(final Configuration conf) {
    if (dataType == DataType.NULL) {
      return Types.NULL;
    }
    if (dataType == DataType.STRING) {
      return isBinary() ? Types.VARBINARY : Types.CHAR;
    }
    if (columnLength <= 0 || getDisplaySize() > 16777215) {
      return isBinary() ? Types.LONGVARBINARY : Types.LONGVARCHAR;
    }
    return isBinary() ? Types.VARBINARY : Types.VARCHAR;
  }

  public String getColumnTypeName(final Configuration conf) {
    switch (dataType) {
      case STRING:
        if (isBinary()) {
          return "BINARY";
        }
        return "CHAR";
      case VARSTRING:
      case VARCHAR:
        if (isBinary()) {
          return "VARBINARY";
        }
        if (columnLength < 0) {
          return "LONGTEXT";
        } else if (getDisplaySize() <= 65532) {
          return "VARCHAR";
        } else if (getDisplaySize() <= 65535) {
          return "TEXT";
        } else if (getDisplaySize() <= 16777215) {
          return "MEDIUMTEXT";
        } else {
          return "LONGTEXT";
        }
      default:
        return dataType.name();
    }
  }

  public int getPrecision() {
    Integer maxWidth = CharsetEncodingLength.maxCharlen.get(charset);
    if (maxWidth == null) {
      return (int) columnLength / 4;
    }
    return (int) (columnLength / maxWidth);
  }

  @Override
  public Object getDefaultText(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException {
    if (isBinary()) {
      byte[] arr = new byte[length.get()];
      buf.readBytes(arr);
      return arr;
    }
    return buf.readString(length.get());
  }

  @Override
  public Object getDefaultBinary(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException {
    if (isBinary()) {
      byte[] arr = new byte[length.get()];
      buf.readBytes(arr);
      return arr;
    }
    return buf.readString(length.get());
  }

  @Override
  public boolean decodeBooleanText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return !"0".equals(buf.readAscii(length.get()));
  }

  @Override
  public boolean decodeBooleanBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return !"0".equals(buf.readAscii(length.get()));
  }

  @Override
  public byte decodeByteText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    if (isBinary()) {
      byte b = buf.readByte();
      if (length.get() > 1) buf.skip(length.get() - 1);
      return b;
    }
    String str = buf.readString(length.get());
    long result;
    try {
      result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValue();
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(
          String.format("value '%s' (%s) cannot be decoded as Byte", str, dataType));
    }
    if ((byte) result != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("byte overflow");
    }
    return (byte) result;
  }

  @Override
  public byte decodeByteBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return decodeByteText(buf, length);
  }

  @Override
  public String decodeStringText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public String decodeStringBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public short decodeShortText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    String str = buf.readString(length.get());
    try {
      return new BigDecimal(str).setScale(0, RoundingMode.DOWN).shortValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Short", str));
    }
  }

  @Override
  public short decodeShortBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return decodeShortText(buf, length);
  }

  @Override
  public int decodeIntText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    String str = buf.readString(length.get());
    try {
      return new BigDecimal(str).setScale(0, RoundingMode.DOWN).intValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Integer", str));
    }
  }

  @Override
  public int decodeIntBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return decodeIntText(buf, length);
  }

  @Override
  public long decodeLongText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    String str = buf.readString(length.get());
    try {
      return new BigInteger(str).longValueExact();
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str));
    }
  }

  @Override
  public long decodeLongBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return decodeLongText(buf, length);
  }

  @Override
  public float decodeFloatText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    String val = buf.readString(length.get());
    try {
      return Float.parseFloat(val);
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Float", val));
    }
  }

  @Override
  public float decodeFloatBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return decodeFloatText(buf, length);
  }

  @Override
  public double decodeDoubleText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    String str2 = buf.readString(length.get());
    try {
      return Double.parseDouble(str2);
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Double", str2));
    }
  }

  @Override
  public double decodeDoubleBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return decodeDoubleText(buf, length);
  }

  @Override
  public Date decodeDateText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException {
    String val = buf.readString(length.get());
    if ("0000-00-00".equals(val)) return null;
    String[] stDatePart = val.split("[- ]");
    if (stDatePart.length < 3) {
      throw new SQLDataException(
          String.format("value '%s' (%s) cannot be decoded as Date", val, dataType));
    }

    try {
      int year = Integer.parseInt(stDatePart[0]);
      int month = Integer.parseInt(stDatePart[1]);
      int dayOfMonth = Integer.parseInt(stDatePart[2]);
      if (cal == null) {
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.MONTH, month - 1);
        c.set(Calendar.DAY_OF_MONTH, dayOfMonth);
        return new Date(c.getTimeInMillis());
      } else {
        synchronized (cal) {
          cal.clear();
          cal.set(Calendar.YEAR, year);
          cal.set(Calendar.MONTH, month - 1);
          cal.set(Calendar.DAY_OF_MONTH, dayOfMonth);
          return new Date(cal.getTimeInMillis());
        }
      }

    } catch (NumberFormatException nfe) {
      throw new SQLDataException(
          String.format("value '%s' (%s) cannot be decoded as Date", val, dataType));
    }
  }

  @Override
  public Date decodeDateBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException {
    return decodeDateText(buf, length, cal, context);
  }

  @Override
  public Time decodeTimeText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException {
    Calendar c = cal == null ? Calendar.getInstance() : cal;
    int offset = c.getTimeZone().getOffset(0);
    int[] parts = LocalTimeCodec.parseTime(buf, length, this);
    long timeInMillis =
        (parts[1] * 3_600_000L + parts[2] * 60_000L + parts[3] * 1_000L + parts[4] / 1_000_000)
                * parts[0]
            - offset;
    return new Time(timeInMillis);
  }

  @Override
  public Time decodeTimeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final Calendar calParam,
      final Context context)
      throws SQLDataException {
    int[] parts = LocalTimeCodec.parseTime(buf, length, this);
    Time t;

    // specific case for TIME, to handle value not in 00:00:00-23:59:59
    if (calParam == null) {
      Calendar cal = Calendar.getInstance();
      cal.clear();
      cal.setLenient(true);
      if (parts[0] == -1) {
        cal.set(
            1970,
            Calendar.JANUARY,
            1,
            parts[0] * parts[1],
            parts[0] * parts[2],
            parts[0] * parts[3] - 1);
        t = new Time(cal.getTimeInMillis() + (1000 - parts[4]));
      } else {
        cal.set(1970, Calendar.JANUARY, 1, parts[1], parts[2], parts[3]);
        t = new Time(cal.getTimeInMillis() + parts[4] / 1_000_000);
      }
    } else {
      synchronized (calParam) {
        calParam.clear();
        calParam.setLenient(true);
        if (parts[0] == -1) {
          calParam.set(
              1970,
              Calendar.JANUARY,
              1,
              parts[0] * parts[1],
              parts[0] * parts[2],
              parts[0] * parts[3] - 1);
          t = new Time(calParam.getTimeInMillis() + (1000 - parts[4]));
        } else {
          calParam.set(1970, Calendar.JANUARY, 1, parts[1], parts[2], parts[3]);
          t = new Time(calParam.getTimeInMillis() + parts[4] / 1_000_000);
        }
      }
    }
    return t;
  }

  @Override
  public Timestamp decodeTimestampText(
      final ReadableByteBuf buf, final MutableInt length, Calendar calParam, final Context context)
      throws SQLDataException {
    int pos = buf.pos();
    int nanoBegin = -1;
    int[] timestampsPart = new int[] {0, 0, 0, 0, 0, 0, 0};
    int partIdx = 0;
    for (int begin = 0; begin < length.get(); begin++) {
      byte b = buf.readByte();
      if (b == '-' || b == ' ' || b == ':') {
        partIdx++;
        continue;
      }
      if (b == '.') {
        partIdx++;
        nanoBegin = begin;
        continue;
      }
      if (b < '0' || b > '9') {
        buf.pos(pos);
        throw new SQLDataException(
            String.format(
                "value '%s' (%s) cannot be decoded as Timestamp",
                buf.readString(length.get()), dataType));
      }

      timestampsPart[partIdx] = timestampsPart[partIdx] * 10 + b - 48;
    }
    if (timestampsPart[0] == 0
        && timestampsPart[1] == 0
        && timestampsPart[2] == 0
        && timestampsPart[3] == 0
        && timestampsPart[4] == 0
        && timestampsPart[5] == 0
        && timestampsPart[6] == 0) {
      length.set(NULL_LENGTH);
      return null;
    }

    // fix non-leading tray for nanoseconds
    if (nanoBegin > 0) {
      for (int begin = 0; begin < 6 - (length.get() - nanoBegin - 1); begin++) {
        timestampsPart[6] = timestampsPart[6] * 10;
      }
    }

    Timestamp timestamp;
    if (calParam == null) {
      Calendar c = Calendar.getInstance();
      c.set(
          timestampsPart[0],
          timestampsPart[1] - 1,
          timestampsPart[2],
          timestampsPart[3],
          timestampsPart[4],
          timestampsPart[5]);
      timestamp = new Timestamp(c.getTime().getTime());
    } else {
      synchronized (calParam) {
        calParam.clear();
        calParam.set(
            timestampsPart[0],
            timestampsPart[1] - 1,
            timestampsPart[2],
            timestampsPart[3],
            timestampsPart[4],
            timestampsPart[5]);
        timestamp = new Timestamp(calParam.getTime().getTime());
      }
    }
    timestamp.setNanos(timestampsPart[6] * 1000);
    return timestamp;
  }

  @Override
  public Timestamp decodeTimestampBinary(
      final ReadableByteBuf buf, final MutableInt length, Calendar calParam, final Context context)
      throws SQLDataException {
    String val = buf.readString(length.get());
    try {
      int[] parts = LocalDateTimeCodec.parseTimestamp(val);
      if (parts == null) {
        length.set(NULL_LENGTH);
        return null;
      }
      int year = parts[0];
      int month = parts[1];
      int dayOfMonth = parts[2];
      int hour = parts[3];
      int minutes = parts[4];
      int seconds = parts[5];
      int microseconds = parts[6] / 1000;
      Timestamp timestamp;
      if (calParam == null) {
        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.set(year, month - 1, dayOfMonth, hour, minutes, seconds);
        timestamp = new Timestamp(cal.getTimeInMillis());
      } else {
        synchronized (calParam) {
          calParam.clear();
          calParam.set(year, month - 1, dayOfMonth, hour, minutes, seconds);
          timestamp = new Timestamp(calParam.getTimeInMillis());
        }
      }
      timestamp.setNanos(microseconds * 1000);
      return timestamp;

    } catch (DateTimeException dte) {
      throw new SQLDataException(
          String.format("value '%s' (%s) cannot be decoded as Timestamp", val, dataType));
    }
  }
}
