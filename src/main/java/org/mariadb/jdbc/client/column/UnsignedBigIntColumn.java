// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.column;

import java.math.BigInteger;
import java.sql.*;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

/** Column metadata definition */
public class UnsignedBigIntColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  public UnsignedBigIntColumn(
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
    return BigInteger.class.getName();
  }

  public int getColumnType(Configuration conf) {
    return Types.BIGINT;
  }

  public String getColumnTypeName(Configuration conf) {
    return "BIGINT UNSIGNED";
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, int length)
      throws SQLDataException {
    return new BigInteger(buf.readAscii(length));
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, int length)
      throws SQLDataException {
    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    return new BigInteger(1, bb);
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, int length) throws SQLDataException {
    String s = buf.readAscii(length);
    return !"0".equals(s);
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return buf.readLong() != 0;
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, int length) throws SQLDataException {
    long result = buf.atoull(length);
    if ((byte) result != result || result < 0) {
      throw new SQLDataException("byte overflow");
    }
    return (byte) result;
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    BigInteger val = new BigInteger(1, bb);
    try {
      return val.byteValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(
          String.format("value '%s' (%s) cannot be decoded as Byte", val, dataType));
    }
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length);
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    return new BigInteger(1, bb).toString();
  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, int length) throws SQLDataException {
    long result = buf.atoull(length);
    if ((short) result != result || result < 0) {
      throw new SQLDataException("Short overflow");
    }
    return (short) result;
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    long result = buf.readLong();
    if ((short) result != result || result < 0) {
      throw new SQLDataException("Short overflow");
    }
    return (short) result;
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, int length) throws SQLDataException {
    long result = buf.atoull(length);
    int res = (int) result;
    if (res != result || result < 0) {
      throw new SQLDataException("integer overflow");
    }
    return res;
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, int length) throws SQLDataException {

    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    BigInteger val = new BigInteger(1, bb);
    try {
      return val.intValueExact();
    } catch (ArithmeticException ae) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Integer", val));
    }
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, int length) throws SQLDataException {
    if (length < 10) return buf.atoull(length);
    BigInteger val = new BigInteger(buf.readAscii(length));
    try {
      return val.longValueExact();
    } catch (ArithmeticException ae) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", val));
    }
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    if ((buf.getByte(buf.pos() + 7) & 0x80) == 0) {
      return buf.readLong();
    } else {
      // error too big to return a long
      byte[] bb = new byte[8];
      for (int i = 7; i >= 0; i--) {
        bb[i] = buf.readByte();
      }
      BigInteger val = new BigInteger(1, bb);
      try {
        return val.longValueExact();
      } catch (ArithmeticException ae) {
        throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", val));
      }
    }
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, int length) throws SQLDataException {
    return Float.parseFloat(buf.readAscii(length));
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    return new BigInteger(1, bb).floatValue();
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, int length) throws SQLDataException {
    return Double.parseDouble(buf.readAscii(length));
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    return new BigInteger(1, bb).doubleValue();
  }

  @Override
  public Date decodeDateText(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
  }

  @Override
  public Date decodeDateBinary(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
  }

  @Override
  public Time decodeTimeText(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Time", dataType));
  }

  @Override
  public Time decodeTimeBinary(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(String.format("Data type %s cannot be decoded as Time", dataType));
  }

  @Override
  public Timestamp decodeTimestampText(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Timestamp", dataType));
  }

  @Override
  public Timestamp decodeTimestampBinary(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    buf.skip(length);
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Timestamp", dataType));
  }
}
