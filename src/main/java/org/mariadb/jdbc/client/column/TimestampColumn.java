// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import static org.mariadb.jdbc.client.result.Result.NULL_LENGTH;

import java.sql.*;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.plugin.codec.LocalDateTimeCodec;

/** Column metadata definition */
public class TimestampColumn extends ColumnDefinitionPacket implements ColumnDecoder {
  private static final DateTimeFormatter dateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  private static final DecimalFormat oldDecimalFormat =
      new DecimalFormat(".0#####", DecimalFormatSymbols.getInstance(Locale.US));

  /**
   * TIMESTAMP metadata type decoder
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
  public TimestampColumn(
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

  /**
   * Recreate new column using alias as name.
   *
   * @param prev current column
   */
  protected TimestampColumn(TimestampColumn prev) {
    super(prev, true);
  }

  @Override
  public TimestampColumn useAliasAsName() {
    return new TimestampColumn(this);
  }

  public String defaultClassname(final Configuration conf) {
    return Timestamp.class.getName();
  }

  public int getColumnType(final Configuration conf) {
    return Types.TIMESTAMP;
  }

  public String getColumnTypeName(final Configuration conf) {
    return dataType.name();
  }

  @Override
  public Object getDefaultText(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException {
    return decodeTimestampText(buf, length, null, context);
  }

  @Override
  public Object getDefaultBinary(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException {
    return decodeTimestampBinary(buf, length, null, context);
  }

  @Override
  public boolean decodeBooleanText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Boolean", dataType));
  }

  @Override
  public boolean decodeBooleanBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Boolean", dataType));
  }

  @Override
  public byte decodeByteText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Byte", dataType));
  }

  @Override
  public byte decodeByteBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Byte", dataType));
  }

  @Override
  public String decodeStringText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final Calendar providedCal,
      final Context context)
      throws SQLDataException {
    if (length.get() == 0) return buildZeroDate();
    int initialLength = length.get();
    LocalDateTime ldt = parseText(buf, length);
    if (ldt == null) {
      if (initialLength > 0) return buildZeroDate();
      return null;
    }
    LocalDateTime modifiedLdt =
        localDateTimeToZoneDateTime(ldt, providedCal, context).toLocalDateTime();
    String timestampWithoutMicro = dateTimeFormatter.format(modifiedLdt);
    if (context.getConf().oldModeNoPrecisionTimestamp()) {
      // for compatibility with 2.2.0 and before, micro precision use .0##### format
      return timestampWithoutMicro
          + oldDecimalFormat.format(((double) modifiedLdt.getNano()) / 1000000000);
    }
    if (this.decimals == 0) return timestampWithoutMicro;
    return timestampWithoutMicro
        + "."
        + String.format(Locale.US, "%0" + this.decimals + "d", modifiedLdt.getNano() / 1000);
  }

  private String buildZeroDate() {
    StringBuilder zeroValue = new StringBuilder("0000-00-00 00:00:00");
    if (this.decimals > 0) {
      zeroValue.append(".");
      for (int i = 0; i < this.decimals; i++) zeroValue.append("0");
    }
    return zeroValue.toString();
  }

  @Override
  public String decodeStringBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final Calendar providedCal,
      final Context context)
      throws SQLDataException {
    if (length.get() == 0) return buildZeroDate();
    int initialLength = length.get();
    LocalDateTime ldt = parseBinary(buf, length);
    if (ldt == null) {
      if (initialLength > 0) return buildZeroDate();
      return null;
    }
    LocalDateTime modifiedLdt =
        localDateTimeToZoneDateTime(ldt, providedCal, context).toLocalDateTime();
    String timestampWithoutMicro = dateTimeFormatter.format(modifiedLdt);
    if (context.getConf().oldModeNoPrecisionTimestamp()) {
      // for compatibility with 2.2.0 and before, micro precision use .0##### format
      return timestampWithoutMicro
          + oldDecimalFormat.format(((double) modifiedLdt.getNano()) / 1000000000);
    }
    if (this.decimals == 0) return timestampWithoutMicro;
    return timestampWithoutMicro
        + "."
        + String.format(Locale.US, "%0" + this.decimals + "d", modifiedLdt.getNano() / 1000);
  }

  @Override
  public short decodeShortText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Short", dataType));
  }

  @Override
  public short decodeShortBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Short", dataType));
  }

  @Override
  public int decodeIntText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Integer", dataType));
  }

  @Override
  public int decodeIntBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Integer", dataType));
  }

  @Override
  public long decodeLongText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
  }

  @Override
  public long decodeLongBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
  }

  @Override
  public float decodeFloatText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Float", dataType));
  }

  @Override
  public float decodeFloatBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Float", dataType));
  }

  @Override
  public double decodeDoubleText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Double", dataType));
  }

  @Override
  public double decodeDoubleBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Double", dataType));
  }

  @Override
  public Date decodeDateText(
      final ReadableByteBuf buf, final MutableInt length, Calendar calParam, Context context)
      throws SQLDataException {
    LocalDateTime ldt = parseText(buf, length);
    if (ldt == null) return null;
    return new Date(localDateTimeToInstant(ldt, calParam, context) + ldt.getNano() / 1_000_000);
  }

  @Override
  public Date decodeDateBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final Calendar calParam,
      final Context context)
      throws SQLDataException {
    LocalDateTime ldt = parseBinary(buf, length);
    if (ldt == null) return null;
    return new Date(localDateTimeToInstant(ldt, calParam, context) + ldt.getNano() / 1_000_000);
  }

  @Override
  public Time decodeTimeText(
      final ReadableByteBuf buf, final MutableInt length, Calendar calParam, Context context)
      throws SQLDataException {
    LocalDateTime ldt = parseText(buf, length);
    if (ldt == null) return null;
    return new Time(
        localDateTimeToInstant(ldt.withYear(1970).withMonth(1).withDayOfMonth(1), calParam, context)
            + ldt.getNano() / 1_000_000);
  }

  @Override
  public Time decodeTimeBinary(
      final ReadableByteBuf buf, final MutableInt length, Calendar calParam, Context context)
      throws SQLDataException {
    LocalDateTime ldt = parseBinary(buf, length);
    if (ldt == null) return null;
    return new Time(
        localDateTimeToInstant(ldt.withYear(1970).withMonth(1).withDayOfMonth(1), calParam, context)
            + ldt.getNano() / 1_000_000);
  }

  @Override
  public Timestamp decodeTimestampText(
      final ReadableByteBuf buf, final MutableInt length, Calendar calParam, final Context context)
      throws SQLDataException {
    LocalDateTime ldt = parseText(buf, length);
    if (ldt == null) return null;
    Timestamp res = new Timestamp(localDateTimeToInstant(ldt, calParam, context));
    res.setNanos(ldt.getNano());
    return res;
  }

  @Override
  public Timestamp decodeTimestampBinary(
      final ReadableByteBuf buf, final MutableInt length, Calendar calParam, final Context context)
      throws SQLDataException {
    LocalDateTime ldt = parseBinary(buf, length);
    if (ldt == null) return null;
    Timestamp res = new Timestamp(localDateTimeToInstant(ldt, calParam, context));
    res.setNanos(ldt.getNano());
    return res;
  }

  private LocalDateTime parseText(final ReadableByteBuf buf, final MutableInt length) {
    int[] parts = LocalDateTimeCodec.parseTextTimestamp(buf, length);
    if (LocalDateTimeCodec.isZeroTimestamp(parts)) {
      length.set(NULL_LENGTH);
      return null;
    }
    return LocalDateTime.of(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
        .plusNanos(parts[6]);
  }

  private LocalDateTime parseBinary(final ReadableByteBuf buf, final MutableInt length) {
    if (length.get() == 0) {
      length.set(NULL_LENGTH);
      return null;
    }

    int year = buf.readUnsignedShort();
    int month = buf.readByte();
    int dayOfMonth = buf.readByte();
    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    long microseconds = 0;

    if (length.get() > 4) {
      hour = buf.readByte();
      minutes = buf.readByte();
      seconds = buf.readByte();

      if (length.get() > 7) {
        microseconds = buf.readUnsignedInt();
      }
    }

    // xpand workaround https://jira.mariadb.org/browse/XPT-274
    if (year == 0
        && month == 0
        && dayOfMonth == 0
        && hour == 0
        && minutes == 0
        && seconds == 0
        && microseconds == 0) {
      length.set(NULL_LENGTH);
      return null;
    }
    return LocalDateTime.of(year, month, dayOfMonth, hour, minutes, seconds)
        .plusNanos(microseconds * 1000);
  }

  public static long localDateTimeToInstant(
      final LocalDateTime ldt, final Calendar calParam, final Context context) {
    if (calParam == null) {
      Calendar cal = context.getDefaultCalendar();
      cal.set(
          ldt.getYear(),
          ldt.getMonthValue() - 1,
          ldt.getDayOfMonth(),
          ldt.getHour(),
          ldt.getMinute(),
          ldt.getSecond());
      cal.set(Calendar.MILLISECOND, 0);
      return cal.getTimeInMillis();
    }
    synchronized (calParam) {
      calParam.clear();
      calParam.set(
          ldt.getYear(),
          ldt.getMonthValue() - 1,
          ldt.getDayOfMonth(),
          ldt.getHour(),
          ldt.getMinute(),
          ldt.getSecond());
      return calParam.getTimeInMillis();
    }
  }

  public static ZonedDateTime localDateTimeToZoneDateTime(
      final LocalDateTime ldt, final Calendar calParam, final Context context) {
    if (calParam == null) {
      if (context.getConf().preserveInstants()) {
        return ldt.atZone(context.getConnectionTimeZone().toZoneId())
            .withZoneSameInstant(TimeZone.getDefault().toZoneId());
      }
      return ldt.atZone(TimeZone.getDefault().toZoneId());
    }
    return ldt.atZone(calParam.getTimeZone().toZoneId());
  }
}
