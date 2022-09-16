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
public class MediumIntColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  public MediumIntColumn(
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
    return Integer.class.getName();
  }

  public int getColumnType(Configuration conf) {
    return Types.INTEGER;
  }

  public String getColumnTypeName(Configuration conf) {
    return isSigned() ? "MEDIUMINT" : "MEDIUMINT UNSIGNED";
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, int length)
      throws SQLDataException {
    return decodeIntText(buf, length);
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, int length)
      throws SQLDataException {
    return decodeIntBinary(buf, length);
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, int length) throws SQLDataException {
    String s = buf.readAscii(length);
    return !"0".equals(s);
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return buf.readInt() != 0;
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
    long result = isSigned() ? buf.readMedium() : buf.readUnsignedMedium();
    buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
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
    String mediumStr = String.valueOf(isSigned() ? buf.readMedium() : buf.readUnsignedMedium());
    buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
    return mediumStr;
  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, int length) throws SQLDataException {
    long result = buf.atoll(length);
    if ((short) result != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("Short overflow");
    }
    return (short) result;
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    long result = isSigned() ? buf.readMedium() : buf.readUnsignedMedium();
    buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
    if ((short) result != result || (result < 0 && !isSigned())) {
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
    int res = isSigned() ? buf.readMedium() : buf.readUnsignedMedium();
    buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
    return res;
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, int length) throws SQLDataException {
    return buf.atoll(length);
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    long l = isSigned() ? buf.readMedium() : buf.readUnsignedMedium();
    buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
    return l;
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, int length) throws SQLDataException {
    return Float.parseFloat(buf.readAscii(length));
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    float f = isSigned() ? buf.readMedium() : buf.readUnsignedMedium();
    buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
    return f;
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, int length) throws SQLDataException {
    return Double.parseDouble(buf.readAscii(length));
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    double f = isSigned() ? buf.readMedium() : buf.readUnsignedMedium();
    buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
    return f;
  }
}
