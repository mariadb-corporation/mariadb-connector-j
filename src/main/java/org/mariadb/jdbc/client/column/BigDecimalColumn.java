// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

/** Column metadata definition */
public class BigDecimalColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  /**
   * Constructor for column corresponding to DECIMAL datatype. Class permit specific decoding for
   * this datatype
   *
   * @param buf Column definition MySQL packet buffer
   * @param charset charset
   * @param length datatype length
   * @param dataType data type
   * @param decimals number of decimals
   * @param flags column flags
   * @param stringPos string value position
   * @param extTypeName extended type name
   * @param extTypeFormat extended type format
   */
  public BigDecimalColumn(
      final ReadableByteBuf buf,
      final int charset,
      final long length,
      final DataType dataType,
      final byte decimals,
      final int flags,
      final int[] stringPos,
      final byte[] extTypeName,
      final byte[] extTypeFormat) {
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
  protected BigDecimalColumn(BigDecimalColumn prev) {
    super(prev, true);
  }

  @Override
  public BigDecimalColumn useAliasAsName() {
    return new BigDecimalColumn(this);
  }

  public String defaultClassname(final Configuration conf) {
    return BigDecimal.class.getName();
  }

  public int getColumnType(final Configuration conf) {
    return Types.DECIMAL;
  }

  public String getColumnTypeName(final Configuration conf) {
    if (isSigned()) return "DECIMAL";
    return "DECIMAL UNSIGNED";
  }

  @Override
  public int getPrecision() {
    // DECIMAL and OLDDECIMAL are  "exact" fixed-point number.
    // so :
    // - if is signed, 1 byte is saved for sign
    // - if decimal > 0, one byte more for dot
    if (isSigned()) {
      return (int) (columnLength - ((decimals > 0) ? 2 : 1));
    } else {
      return (int) (columnLength - ((decimals > 0) ? 1 : 0));
    }
  }

  @Override
  public Object getDefaultText(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException {
    return new BigDecimal(buf.readAscii(length.get()));
  }

  @Override
  public Object getDefaultBinary(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException {
    return new BigDecimal(buf.readAscii(length.get()));
  }

  @Override
  public boolean decodeBooleanText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return new BigDecimal(buf.readAscii(length.get())).intValue() != 0;
  }

  @Override
  public boolean decodeBooleanBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return decodeBooleanText(buf, length);
  }

  @Override
  public byte decodeByteText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    String str = buf.readString(length.get());
    try {
      return new BigDecimal(str).setScale(0, RoundingMode.DOWN).byteValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(
          String.format("value '%s' (%s) cannot be decoded as Byte", str, dataType));
    }
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
  public short decodeShortBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return decodeShortText(buf, length);
  }

  @Override
  public int decodeIntText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
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
  public int decodeIntBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return decodeIntText(buf, length);
  }

  @Override
  public long decodeLongText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    String str2 = buf.readAscii(length.get());
    try {
      return new BigDecimal(str2).setScale(0, RoundingMode.DOWN).longValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str2));
    }
  }

  @Override
  public long decodeLongBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    String str = buf.readString(length.get());
    try {
      return new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str));
    }
  }

  @Override
  public float decodeFloatText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return Float.parseFloat(buf.readAscii(length.get()));
  }

  @Override
  public float decodeFloatBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return new BigDecimal(buf.readAscii(length.get())).floatValue();
  }

  @Override
  public double decodeDoubleText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return Double.parseDouble(buf.readAscii(length.get()));
  }

  @Override
  public double decodeDoubleBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return new BigDecimal(buf.readAscii(length.get())).doubleValue();
  }

  @Override
  public Date decodeDateText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
  }

  @Override
  public Date decodeDateBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
  }

  @Override
  public Time decodeTimeText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Time", dataType));
  }

  @Override
  public Time decodeTimeBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Time", dataType));
  }

  @Override
  public Timestamp decodeTimestampText(
      final ReadableByteBuf buf, final MutableInt length, Calendar cal, final Context context)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Timestamp", dataType));
  }

  @Override
  public Timestamp decodeTimestampBinary(
      final ReadableByteBuf buf, final MutableInt length, Calendar cal, final Context context)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Timestamp", dataType));
  }
}
