// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

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
public class UnsignedTinyIntColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  /**
   * TINYINT UNSIGNED metadata type decoder
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
  public UnsignedTinyIntColumn(
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
  protected UnsignedTinyIntColumn(UnsignedTinyIntColumn prev) {
    super(prev, true);
  }

  @Override
  public UnsignedTinyIntColumn useAliasAsName() {
    return new UnsignedTinyIntColumn(this);
  }

  public String defaultClassname(final Configuration conf) {
    if (conf.tinyInt1isBit() && columnLength == 1) return Boolean.class.getName();
    return Integer.class.getName();
  }

  public int getColumnType(final Configuration conf) {
    if (conf.tinyInt1isBit() && columnLength == 1) {
      return conf.transformedBitIsBoolean() ? Types.BOOLEAN : Types.BIT;
    }
    return Types.SMALLINT;
  }

  public String getColumnTypeName(final Configuration conf) {
    if (conf.tinyInt1isBit() && columnLength == 1) {
      return conf.transformedBitIsBoolean() ? "BOOLEAN" : "BIT";
    }
    return "TINYINT UNSIGNED";
  }

  @Override
  public Object getDefaultText(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException {
    if (context.getConf().tinyInt1isBit() && columnLength == 1) {
      return decodeBooleanText(buf, length);
    }
    return (int) buf.atoull(length.get());
  }

  @Override
  public Object getDefaultBinary(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException {
    if (context.getConf().tinyInt1isBit() && columnLength == 1) {
      return decodeBooleanBinary(buf, length);
    }
    return (int) buf.readUnsignedByte();
  }

  @Override
  public boolean decodeBooleanText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    String s = buf.readAscii(length.get());
    return !"0".equals(s);
  }

  @Override
  public boolean decodeBooleanBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return buf.readByte() != 0;
  }

  @Override
  public byte decodeByteText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    long result = buf.atoull(length.get());
    if ((byte) result != result) {
      throw new SQLDataException("byte overflow");
    }
    return (byte) result;
  }

  @Override
  public byte decodeByteBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    long result = buf.readUnsignedByte();
    if ((byte) result != result) {
      throw new SQLDataException("byte overflow");
    }
    return (byte) result;
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
    return String.valueOf(buf.readUnsignedByte());
  }

  @Override
  public short decodeShortText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return (short) buf.atoull(length.get());
  }

  @Override
  public short decodeShortBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return buf.readUnsignedByte();
  }

  @Override
  public int decodeIntText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return (int) buf.atoull(length.get());
  }

  @Override
  public int decodeIntBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return buf.readUnsignedByte();
  }

  @Override
  public long decodeLongText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return buf.atoull(length.get());
  }

  @Override
  public long decodeLongBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return buf.readUnsignedByte();
  }

  @Override
  public float decodeFloatText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return Float.parseFloat(buf.readAscii(length.get()));
  }

  @Override
  public float decodeFloatBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return buf.readUnsignedByte();
  }

  @Override
  public double decodeDoubleText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return Double.parseDouble(buf.readAscii(length.get()));
  }

  @Override
  public double decodeDoubleBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return buf.readUnsignedByte();
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
