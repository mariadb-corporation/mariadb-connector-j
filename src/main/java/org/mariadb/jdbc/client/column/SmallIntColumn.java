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
public class SmallIntColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  public SmallIntColumn(
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
    return isSigned() ? Short.class.getName() : Integer.class.getName();
  }

  public int getColumnType(Configuration conf) {
    return isSigned() ? Types.SMALLINT : Types.INTEGER;
  }

  public String getColumnTypeName(Configuration conf) {
    return isSigned() ? "SMALLINT" : "SMALLINT UNSIGNED";
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, int length)
      throws SQLDataException {
    if (isSigned()) {
      return (short) buf.atoll(length);
    }
    return (int) buf.atoull(length);
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, int length)
      throws SQLDataException {
    if (isSigned()) {
      return buf.readShort();
    }
    return buf.readUnsignedShort();
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, int length) throws SQLDataException {
    String s = buf.readAscii(length);
    return !"0".equals(s);
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return buf.readShort() != 0;
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, int length) throws SQLDataException {
    long result = buf.atoll(length);
    if ((byte) result != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("byte overflow");
    }
    return (byte) result;
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    long result = isSigned() ? buf.readShort() : buf.readUnsignedShort();
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
      return String.valueOf(buf.readUnsignedShort());
    }
    return String.valueOf(buf.readShort());
  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, int length) throws SQLDataException {
    if (isSigned()) {
      return (short) buf.atoll(length);
    }
    long result = buf.atoull(length);
    if ((short) result != result) {
      throw new SQLDataException("Short overflow");
    }
    return (short) result;
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    if (isSigned()) {
      return buf.readShort();
    }
    int result = buf.readUnsignedShort();
    if ((short) result != result) {
      throw new SQLDataException("Short overflow");
    }
    return (short) result;
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, int length) throws SQLDataException {
    return (int) buf.atoll(length);
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return isSigned() ? buf.readShort() : buf.readUnsignedShort();
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, int length) throws SQLDataException {
    return buf.atoll(length);
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return isSigned() ? buf.readShort() : buf.readUnsignedShort();
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, int length) throws SQLDataException {
    return Float.parseFloat(buf.readAscii(length));
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    if (!isSigned()) {
      return (float) buf.readUnsignedShort();
    }
    return buf.readShort();
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, int length) throws SQLDataException {
    return Double.parseDouble(buf.readAscii(length));
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    if (!isSigned()) {
      return buf.readUnsignedShort();
    }
    return buf.readShort();
  }
}
