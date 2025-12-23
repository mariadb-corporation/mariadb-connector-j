// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import java.math.BigInteger;
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
public class UnsignedBigIntColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  /**
   * BIGINT UNSIGNED metadata type decoder
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
  public UnsignedBigIntColumn(
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
  protected UnsignedBigIntColumn(UnsignedBigIntColumn prev) {
    super(prev, true);
  }

  @Override
  public UnsignedBigIntColumn useAliasAsName() {
    return new UnsignedBigIntColumn(this);
  }

  public String defaultClassname(final Configuration conf) {
    return BigInteger.class.getName();
  }

  public int getColumnType(final Configuration conf) {
    return Types.BIGINT;
  }

  public String getColumnTypeName(final Configuration conf) {
    return "BIGINT UNSIGNED";
  }

  @Override
  public Object getDefaultText(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException {
    return new BigInteger(buf.readAscii(length.get()));
  }

  @Override
  public Object getDefaultBinary(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException {
    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    return new BigInteger(1, bb);
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
    return buf.readLong() != 0;
  }

  @Override
  public byte decodeByteText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    long result = buf.atoull(length.get());
    if ((byte) result != result || result < 0) {
      throw new SQLDataException("byte overflow");
    }
    return (byte) result;
  }

  @Override
  public byte decodeByteBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
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
  public String decodeStringText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public String decodeStringBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException {
    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    return new BigInteger(1, bb).toString();
  }

  @Override
  public short decodeShortText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    long result = buf.atoull(length.get());
    if ((short) result != result || result < 0) {
      throw new SQLDataException("Short overflow");
    }
    return (short) result;
  }

  @Override
  public short decodeShortBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    long result = buf.readLong();
    if ((short) result != result || result < 0) {
      throw new SQLDataException("Short overflow");
    }
    return (short) result;
  }

  @Override
  public int decodeIntText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    long result = buf.atoull(length.get());
    int res = (int) result;
    if (res != result || result < 0) {
      throw new SQLDataException("integer overflow");
    }
    return res;
  }

  @Override
  public int decodeIntBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {

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
  public long decodeLongText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    if (length.get() < 10) return buf.atoull(length.get());
    BigInteger val = new BigInteger(buf.readAscii(length.get()));
    try {
      return val.longValueExact();
    } catch (ArithmeticException ae) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", val));
    }
  }

  @Override
  public long decodeLongBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
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
  public float decodeFloatText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return Float.parseFloat(buf.readAscii(length.get()));
  }

  @Override
  public float decodeFloatBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    return new BigInteger(1, bb).floatValue();
  }

  @Override
  public double decodeDoubleText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    return Double.parseDouble(buf.readAscii(length.get()));
  }

  @Override
  public double decodeDoubleBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException {
    // need BIG ENDIAN, so reverse order
    byte[] bb = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bb[i] = buf.readByte();
    }
    return new BigInteger(1, bb).doubleValue();
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
