// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import java.sql.*;
import java.util.Calendar;
import java.util.UUID;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.util.CharsetEncodingLength;

/** Column metadata definition */
public class UuidColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  /**
   * UUID metadata type decoder
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
  public UuidColumn(
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
  protected UuidColumn(UuidColumn prev) {
    super(prev, true);
  }

  @Override
  public UuidColumn useAliasAsName() {
    return new UuidColumn(this);
  }

  public String defaultClassname(final Configuration conf) {
    return conf.uuidAsString() ? String.class.getName() : UUID.class.getName();
  }

  public int getColumnType(final Configuration conf) {
    return conf.uuidAsString() ? Types.CHAR : Types.OTHER;
  }

  public String getColumnTypeName(final Configuration conf) {
    return "uuid";
  }

  public int getPrecision() {
    Integer maxWidth = CharsetEncodingLength.maxCharlen.get(charset);
    if (maxWidth == null) {
      return (int) columnLength;
    }
    return (int) (columnLength / maxWidth);
  }

  @Override
  public Object getDefaultText(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException {
    return context.getConf().uuidAsString()
        ? buf.readString(length.get())
        : UUID.fromString(buf.readAscii(length.get()));
  }

  @Override
  public Object getDefaultBinary(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException {
    return context.getConf().uuidAsString()
        ? buf.readString(length.get())
        : UUID.fromString(buf.readAscii(length.get()));
  }

  @Override
  public boolean decodeBooleanText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Boolean");
  }

  @Override
  public boolean decodeBooleanBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Boolean");
  }

  @Override
  public byte decodeByteText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as byte");
  }

  @Override
  public byte decodeByteBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as byte");
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
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Short");
  }

  @Override
  public short decodeShortBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Short");
  }

  @Override
  public int decodeIntText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Integer");
  }

  @Override
  public int decodeIntBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Integer");
  }

  @Override
  public long decodeLongText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Long");
  }

  @Override
  public long decodeLongBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Long");
  }

  @Override
  public float decodeFloatText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Float");
  }

  @Override
  public float decodeFloatBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Float");
  }

  @Override
  public double decodeDoubleText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Double");
  }

  @Override
  public double decodeDoubleBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Double");
  }

  @Override
  public Date decodeDateText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Date");
  }

  @Override
  public Date decodeDateBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Date");
  }

  @Override
  public Time decodeTimeText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Time");
  }

  @Override
  public Time decodeTimeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final Calendar calParam,
      final Context context)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Time");
  }

  @Override
  public Timestamp decodeTimestampText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final Calendar calParam,
      final Context context)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Timestamp");
  }

  @Override
  public Timestamp decodeTimestampBinary(
      final ReadableByteBuf buf, final MutableInt length, Calendar calParam, final Context context)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException("Data type UUID cannot be decoded as Timestamp");
  }
}
