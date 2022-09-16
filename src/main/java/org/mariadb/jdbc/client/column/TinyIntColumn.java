// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.column;

import java.sql.SQLDataException;
import java.sql.Types;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

/** Column metadata definition */
public class TinyIntColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  public TinyIntColumn(
      ReadableByteBuf buf,
      int charset,
      long length,
      DataType dataType,
      byte decimals,
      int flags,
      int[] stringPos,
      String extTypeName,
      String extTypeFormat) {
    super(buf, charset, length, dataType, decimals, flags, stringPos, extTypeName, extTypeFormat);
  }

  public String defaultClassname(Configuration conf) {
    if (conf.tinyInt1isBit() && columnLength == 1) return Boolean.class.getName();
    return Integer.class.getName();
  }

  public int getColumnType(Configuration conf) {
    if (conf.tinyInt1isBit() && columnLength == 1) {
      return conf.transformedBitIsBoolean() ? Types.BOOLEAN : Types.BIT;
    }
    return isSigned() ? Types.TINYINT : Types.SMALLINT;
  }

  public String getColumnTypeName(Configuration conf) {
    if (conf.tinyInt1isBit() && columnLength == 1) {
      return conf.transformedBitIsBoolean() ? "BOOLEAN" : "BIT";
    }
    return isSigned() ? "TINYINT" : "TINYINT UNSIGNED";
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, int length)
      throws SQLDataException {
    if (conf.tinyInt1isBit() && columnLength == 1) {
      return decodeBooleanText(buf, length);
    }
    return (int) buf.atoll(length);
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, int length)
      throws SQLDataException {
    if (conf.tinyInt1isBit() && columnLength == 1) {
      return decodeBooleanBinary(buf, length);
    }
    if (isSigned()) {
      return (int) buf.readByte();
    }
    return (int) buf.readUnsignedByte();
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, int length) throws SQLDataException {
    String s = buf.readAscii(length);
    return !"0".equals(s);
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return buf.readByte() != 0;
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, int length) throws SQLDataException {
    long result = buf.atoll(length);
    if ((byte) result != result) {
      throw new SQLDataException("byte overflow");
    }
    return (byte) result;
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    if (isSigned()) return buf.readByte();
    long result = buf.readUnsignedByte();

    if ((byte) result != result) {
      throw new SQLDataException("byte overflow");
    }
    return (byte) result;
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length);
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    if (!isSigned()) {
      return String.valueOf(buf.readUnsignedByte());
    }
    return String.valueOf(buf.readByte());
  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, int length) throws SQLDataException {
    return (short) buf.atoll(length);
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return (isSigned() ? buf.readByte() : buf.readUnsignedByte());
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, int length) throws SQLDataException {
    return (int) buf.atoll(length);
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return (isSigned() ? buf.readByte() : buf.readUnsignedByte());
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, int length) throws SQLDataException {
    return buf.atoll(length);
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    if (!isSigned()) {
      return buf.readUnsignedByte();
    }
    return buf.readByte();
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, int length) throws SQLDataException {
    return Float.parseFloat(buf.readAscii(length));
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    if (!isSigned()) {
      return buf.readUnsignedByte();
    }
    return buf.readByte();
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, int length) throws SQLDataException {
    return Double.parseDouble(buf.readAscii(length));
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    if (!isSigned()) {
      return buf.readUnsignedByte();
    }
    return buf.readByte();
  }
}
