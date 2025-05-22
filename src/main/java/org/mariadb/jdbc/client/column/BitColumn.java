// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import java.io.IOException;
import java.sql.*;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.impl.readable.BufferedReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.plugin.codec.ByteCodec;

/** Column metadata definition */
public class BitColumn extends ColumnDefinitionPacket implements ColumnDecoder {
  /**
   * Constructor for column corresponding to BIT datatype. Class permit specific decoding for this
   * datatype
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
  public BitColumn(
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
        (BufferedReadableByteBuf) buf,
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
  protected BitColumn(BitColumn prev) {
    super(prev, true);
  }

  @Override
  public BitColumn useAliasAsName() {
    return new BitColumn(this);
  }

  public String defaultClassname(final Configuration conf) {
    return columnLength == 1 && conf.transformedBitIsBoolean() ? Boolean.class.getName() : "byte[]";
  }

  public int getColumnType(final Configuration conf) {
    return columnLength == 1 && conf.transformedBitIsBoolean() ? Types.BOOLEAN : Types.BIT;
  }

  public String getColumnTypeName(final Configuration conf) {
    return "BIT";
  }

  @Override
  public int getPrecision() {
    return (int) columnLength;
  }

  @Override
  public Object getDefaultText(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException, IOException {
    if (columnLength == 1 && context.getConf().transformedBitIsBoolean()) {
      return ByteCodec.parseBit(buf, length) != 0;
    }
    byte[] arr = new byte[length.get()];
    buf.readBytes(arr);
    return arr;
  }

  @Override
  public Object getDefaultBinary(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException, IOException {
    return getDefaultText(buf, length, context);
  }

  @Override
  public boolean decodeBooleanText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    return ByteCodec.parseBit(buf, length) != 0;
  }

  @Override
  public boolean decodeBooleanBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    return ByteCodec.parseBit(buf, length) != 0;
  }

  @Override
  public byte decodeByteText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    byte val = buf.readByte();
    if (length.get() > 1) buf.skip(length.get() - 1);
    return val;
  }

  @Override
  public byte decodeByteBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    return decodeByteText(buf, length);
  }

  @Override
  public String decodeStringText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    if (columnLength == 1 && context.getConf().transformedBitIsBoolean()) {
        return String.valueOf(ByteCodec.parseBit(buf, length) != 0);
    }

    byte[] bytes = new byte[length.get()];
    buf.readBytes(bytes);
    StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE + 3);
    sb.append("b'");
    boolean firstByteNonZero = false;
    for (int i = 0; i < Byte.SIZE * bytes.length; i++) {
      boolean b = (bytes[i / Byte.SIZE] & 1 << (Byte.SIZE - 1 - (i % Byte.SIZE))) > 0;
      if (b) {
        sb.append('1');
        firstByteNonZero = true;
      } else if (firstByteNonZero) {
        sb.append('0');
      }
    }
    sb.append("'");
    return sb.toString();
  }

  @Override
  public String decodeStringBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    if (columnLength == 1 && context.getConf().transformedBitIsBoolean()) {
        return String.valueOf(ByteCodec.parseBit(buf, length) != 0);
    }
    byte[] bytes = new byte[length.get()];
    buf.readBytes(bytes);
    StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE + 3);
    sb.append("b'");
    boolean firstByteNonZero = false;
    for (int i = 0; i < Byte.SIZE * bytes.length; i++) {
      boolean b = (bytes[i / Byte.SIZE] & 1 << (Byte.SIZE - 1 - (i % Byte.SIZE))) > 0;
      if (b) {
        sb.append('1');
        firstByteNonZero = true;
      } else if (firstByteNonZero) {
        sb.append('0');
      }
    }
    sb.append("'");
    return sb.toString();
  }

  @Override
  public short decodeShortText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    long result = 0;
    for (int i = 0; i < length.get(); i++) {
      byte b = buf.readByte();
      result = (result << 8) + (b & 0xff);
    }
    if ((short) result != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("Short overflow");
    }
    return (short) result;
  }

  @Override
  public short decodeShortBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    return decodeShortText(buf, length);
  }

  @Override
  public int decodeIntText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    long result = 0;
    for (int i = 0; i < length.get(); i++) {
      byte b = buf.readByte();
      result = (result << 8) + (b & 0xff);
    }
    int res = (int) result;
    if (res != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("integer overflow");
    }
    return res;
  }

  @Override
  public int decodeIntBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    long result = 0;
    for (int i = 0; i < length.get(); i++) {
      byte b = buf.readByte();
      result = (result << 8) + (b & 0xff);
    }

    int res = (int) result;
    if (res != result) {
      throw new SQLDataException("integer overflow");
    }

    return res;
  }

  @Override
  public long decodeLongText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    long result = 0;
    for (int i = 0; i < length.get(); i++) {
      byte b = buf.readByte();
      result = (result << 8) + (b & 0xff);
    }
    return result;
  }

  @Override
  public long decodeLongBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    return decodeLongText(buf, length);
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
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
  }

  @Override
  public Date decodeDateBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
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
      final ReadableByteBuf buf, final MutableInt length, Calendar cal, final Context context)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Timestamp", dataType));
  }

  @Override
  public Timestamp decodeTimestampBinary(
      final ReadableByteBuf buf, final MutableInt length, Calendar cal, final Context context)
      throws SQLDataException, IOException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Timestamp", dataType));
  }
}
