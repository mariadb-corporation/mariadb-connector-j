// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client.column;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.client.ColumnDecoder;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.util.MutableInt;
import com.singlestore.jdbc.message.server.ColumnDefinitionPacket;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

/** Column metadata definition */
public class DoubleColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  /**
   * Double metadata type decoder
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
  public DoubleColumn(
      ReadableByteBuf buf,
      int charset,
      long length,
      DataType dataType,
      byte decimals,
      int flags,
      int[] stringPos,
      String extTypeName,
      String extTypeFormat) {
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

  protected DoubleColumn(DoubleColumn prev) {
    super(prev, true);
  }

  @Override
  public DoubleColumn useAliasAsName() {
    return new DoubleColumn(this);
  }

  @Override
  public String defaultClassname(Configuration conf) {
    return Double.class.getName();
  }

  @Override
  public int getColumnType(Configuration conf) {
    return Types.DOUBLE;
  }

  @Override
  public String getColumnTypeName(Configuration conf) {
    if (isSigned()) return dataType.name();
    return dataType.name() + " UNSIGNED";
  }

  @Override
  public int getPrecision() {
    return 22;
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return Double.valueOf(buf.readAscii(length.get()));
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return buf.readDouble();
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return new BigDecimal(buf.readAscii(length.get())).intValue() != 0;
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return ((int) buf.readDouble()) != 0;
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result;
    String str = buf.readString(length.get());
    try {
      result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).byteValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(
          String.format("value '%s' (%s) cannot be decoded as Byte", str, dataType));
    }
    if ((byte) result != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("byte overflow");
    }
    return (byte) result;
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result = (long) buf.readDouble();
    if ((byte) result != result) {
      throw new SQLDataException("byte overflow");
    }

    return (byte) result;
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    return String.valueOf(buf.readDouble());
  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result;
    String str = buf.readString(length.get());
    try {
      result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Short", str));
    }
    if ((short) result != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("Short overflow");
    }

    return (short) result;
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result = (long) buf.readDouble();
    if ((short) result != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("Short overflow");
    }
    return (short) result;
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result;
    String str = buf.readString(length.get());
    try {
      result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Integer", str));
    }

    int res = (int) result;
    if (res != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("integer overflow");
    }
    return res;
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result = (long) buf.readDouble();
    int res = (int) result;
    if (res != result) {
      throw new SQLDataException("integer overflow");
    }
    return res;
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    String str2 = buf.readAscii(length.get());
    try {
      return new BigDecimal(str2).setScale(0, RoundingMode.DOWN).longValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str2));
    }
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return (long) buf.readDouble();
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return Float.parseFloat(buf.readAscii(length.get()));
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return (float) buf.readDouble();
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return Double.parseDouble(buf.readAscii(length.get()));
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return buf.readDouble();
  }

  @Override
  public Date decodeDateText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
  }

  @Override
  public Date decodeDateBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
  }

  @Override
  public Time decodeTimeText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Time", dataType));
  }

  @Override
  public Time decodeTimeBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Time", dataType));
  }

  @Override
  public Timestamp decodeTimestampText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Timestamp", dataType));
  }

  @Override
  public Timestamp decodeTimestampBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Timestamp", dataType));
  }
}
