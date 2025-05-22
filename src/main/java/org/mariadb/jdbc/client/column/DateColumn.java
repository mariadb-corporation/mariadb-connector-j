// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import static org.mariadb.jdbc.client.result.Result.NULL_LENGTH;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.TimeZone;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.impl.readable.BufferedReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

/** Column metadata definition */
public class DateColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  /**
   * Date metadata type decoder
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
  public DateColumn(
      final BufferedReadableByteBuf buf,
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

  /**
   * Recreate new column using alias as name.
   *
   * @param prev current column
   */
  protected DateColumn(DateColumn prev) {
    super(prev, true);
  }

  @Override
  public DateColumn useAliasAsName() {
    return new DateColumn(this);
  }

  public String defaultClassname(final Configuration conf) {
    return Date.class.getName();
  }

  public int getColumnType(final Configuration conf) {
    return Types.DATE;
  }

  public String getColumnTypeName(final Configuration conf) {
    return "DATE";
  }

  @Override
  public Object getDefaultText(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException, IOException {
    return decodeDateText(buf, length, null, context);
  }

  @Override
  public Object getDefaultBinary(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException, IOException {
    return decodeDateBinary(buf, length, null, context);
  }

  @Override
  public boolean decodeBooleanText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Boolean", dataType));
  }

  @Override
  public boolean decodeBooleanBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Boolean", dataType));
  }

  @Override
  public byte decodeByteText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Byte", dataType));
  }

  @Override
  public byte decodeByteBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Byte", dataType));
  }

  @Override
  public String decodeStringText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    return buf.readString(length.get());
  }

  @Override
  public String decodeStringBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    if (length.get() == 0) return "0000-00-00";
    int dateYear = buf.readUnsignedShort();
    int dateMonth = buf.readByte();
    int dateDay = buf.readByte();
    return LocalDate.of(dateYear, dateMonth, dateDay).toString();
  }

  @Override
  public short decodeShortText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Short", dataType));
  }

  @Override
  public short decodeShortBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Short", dataType));
  }

  @Override
  public int decodeIntText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Integer", dataType));
  }

  @Override
  public int decodeIntBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Integer", dataType));
  }

  @Override
  public long decodeLongText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
  }

  @Override
  public long decodeLongBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
  }

  @Override
  public float decodeFloatText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Float", dataType));
  }

  @Override
  public float decodeFloatBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Float", dataType));
  }

  @Override
  public double decodeDoubleText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Double", dataType));
  }

  @Override
  public double decodeDoubleBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Double", dataType));
  }

  @Override
  public Date decodeDateText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    int year = (int) buf.atoull(4);
    buf.skip(1);
    int month = (int) buf.atoull(2);
    buf.skip(1);
    int dayOfMonth = (int) buf.atoull(2);
    if (year == 0 && month == 0 && dayOfMonth == 0) {
      length.set(NULL_LENGTH);
      return null;
    }

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
  }

  @Override
  public Date decodeDateBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    if (length.get() == 0) {
      length.set(NULL_LENGTH);
      return null;
    }

    if (cal == null) {
      Calendar c = Calendar.getInstance();
      c.clear();
      c.set(Calendar.YEAR, buf.readShort());
      c.set(Calendar.MONTH, buf.readByte() - 1);
      c.set(Calendar.DAY_OF_MONTH, buf.readByte());
      return new Date(c.getTimeInMillis());
    } else {
      synchronized (cal) {
        cal.clear();
        cal.set(Calendar.YEAR, buf.readShort());
        cal.set(Calendar.MONTH, buf.readByte() - 1);
        cal.set(Calendar.DAY_OF_MONTH, buf.readByte());
        return new Date(cal.getTimeInMillis());
      }
    }
  }

  @Override
  public Time decodeTimeText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Time", dataType));
  }

  @Override
  public Time decodeTimeBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Time", dataType));
  }

  @Override
  public Timestamp decodeTimestampText(
      final ReadableByteBuf buf, final MutableInt length, Calendar calParam, final Context context)
      throws SQLDataException, IOException {
    if (calParam == null || calParam.getTimeZone().equals(TimeZone.getDefault())) {
      String s = buf.readAscii(length.get());
      if ("0000-00-00".equals(s)) {
        length.set(NULL_LENGTH);
        return null;
      }
      return new Timestamp(Date.valueOf(s).getTime());
    }

    String[] datePart = buf.readAscii(length.get()).split("-");
    synchronized (calParam) {
      calParam.clear();
      calParam.set(
          Integer.parseInt(datePart[0]),
          Integer.parseInt(datePart[1]) - 1,
          Integer.parseInt(datePart[2]));
      return new Timestamp(calParam.getTimeInMillis());
    }
  }

  @Override
  public Timestamp decodeTimestampBinary(
      final ReadableByteBuf buf, final MutableInt length, Calendar calParam, final Context context)
      throws SQLDataException, IOException {
    if (length.get() == 0) {
      length.set(NULL_LENGTH);
      return null;
    }

    int year;
    int month;
    long dayOfMonth;

    year = buf.readUnsignedShort();
    month = buf.readByte();
    dayOfMonth = buf.readByte();

    if (year == 0 && month == 0 && dayOfMonth == 0) {
      length.set(NULL_LENGTH);
      return null;
    }

    Timestamp timestamp;
    if (calParam == null) {
      Calendar cal = Calendar.getInstance();
      cal.clear();
      cal.set(year, month - 1, (int) dayOfMonth, 0, 0, 0);
      timestamp = new Timestamp(cal.getTimeInMillis());
    } else {
      synchronized (calParam) {
        calParam.clear();
        calParam.set(year, month - 1, (int) dayOfMonth, 0, 0, 0);
        timestamp = new Timestamp(calParam.getTimeInMillis());
      }
    }
    timestamp.setNanos(0);
    return timestamp;
  }
}
