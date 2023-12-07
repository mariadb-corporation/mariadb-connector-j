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
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

/** Column metadata definition */
public class UnsignedSmallIntColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  /**
   * SMALLINT UNSIGNED metadata type decoder
   *
   * @param buf buffer
   * @param charset charset
   * @param length maximum data length
   * @param dataType data type
   * @param decimals decimal length
   * @param flags flags
   * @param stringPos string offset position in buffer
   * @param extTypeName extended type name
   * @param extTypeFormat extended type format
   */
  public UnsignedSmallIntColumn(
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

  protected UnsignedSmallIntColumn(UnsignedSmallIntColumn prev) {
    super(prev, true);
  }

  @Override
  public UnsignedSmallIntColumn useAliasAsName() {
    return new UnsignedSmallIntColumn(this);
  }

  @Override
  public String defaultClassname(Configuration conf) {
    return Integer.class.getName();
  }

  @Override
  public int getColumnType(Configuration conf) {
    return Types.INTEGER;
  }

  @Override
  public String getColumnTypeName(Configuration conf) {
    return dataType.name() + " UNSIGNED";
  }

  @Override
  public int getPrecision() {
    return 5;
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return (int) buf.atoull(length.get());
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return buf.readUnsignedShort();
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    String s = buf.readAscii(length.get());
    return !"0".equals(s);
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return buf.readShort() != 0;
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result = buf.atoull(length.get());
    if ((byte) result != result || result < 0) {
      throw new SQLDataException("byte overflow");
    }
    return (byte) result;
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result = buf.readUnsignedShort();
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
    return String.valueOf(buf.readUnsignedShort());
  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result = buf.atoull(length.get());
    if ((short) result != result) {
      throw new SQLDataException("Short overflow");
    }
    return (short) result;
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    int result = buf.readUnsignedShort();
    if ((short) result != result) {
      throw new SQLDataException("Short overflow");
    }
    return (short) result;
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return (int) buf.atoull(length.get());
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return buf.readUnsignedShort();
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return buf.atoull(length.get());
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return buf.readUnsignedShort();
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return Float.parseFloat(buf.readAscii(length.get()));
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return (float) buf.readUnsignedShort();
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return Double.parseDouble(buf.readAscii(length.get()));
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return buf.readUnsignedShort();
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
