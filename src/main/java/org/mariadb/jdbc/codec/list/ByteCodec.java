/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.codec.list;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class ByteCodec implements Codec<Byte> {

  public static final ByteCodec INSTANCE = new ByteCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.BIGINT,
          DataType.YEAR,
          DataType.BIT,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.OLDDECIMAL,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.DECIMAL,
          DataType.ENUM,
          DataType.VARSTRING,
          DataType.STRING,
          DataType.VARCHAR);

  public static long parseBit(ReadableByteBuf buf, int length) {
    if (length == 1) {
      return buf.readUnsignedByte();
    }
    long val = 0;
    int idx = 0;
    do {
      val += ((long) buf.readUnsignedByte()) << (8 * length);
      idx++;
    } while (idx < length);
    return val;
  }

  public String className() {
    return Byte.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Byte.TYPE) || type.isAssignableFrom(Byte.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Byte;
  }

  @Override
  public Byte decodeText(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {

    long result;
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case YEAR:
        result = LongCodec.parseNotEmpty(buf, length);
        break;

      case BIT:
        Byte val = buf.readByte();
        if (length > 1) buf.skip(length - 1);
        return val;

      case FLOAT:
      case DOUBLE:
      case OLDDECIMAL:
      case DECIMAL:
      case ENUM:
      case VARCHAR:
      case VARSTRING:
      case STRING:
        String str = buf.readString(length);
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).byteValueExact();
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw new SQLDataException(
              String.format("value '%s' (%s) cannot be decoded as Byte", str, column.getType()));
        }
        break;

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (length > 0) {
          byte b = buf.readByte();
          buf.skip(length - 1);
          return b;
        }
        throw new SQLDataException("empty String value cannot be decoded as Byte");

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Byte", column.getType()));
    }

    if ((byte) result != result || (result < 0 && !column.isSigned())) {
      throw new SQLDataException("byte overflow");
    }

    return (byte) result;
  }

  @Override
  public Byte decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {

    long result;
    switch (column.getType()) {
      case TINYINT:
        result = column.isSigned() ? buf.readByte() : buf.readUnsignedByte();
        break;

      case YEAR:
      case SMALLINT:
        result = column.isSigned() ? buf.readShort() : buf.readUnsignedShort();
        break;

      case MEDIUMINT:
        result = column.isSigned() ? buf.readMedium() : buf.readUnsignedMedium();
        buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
        break;

      case INTEGER:
        result = column.isSigned() ? buf.readInt() : buf.readUnsignedInt();
        break;

      case BIGINT:
        if (column.isSigned()) {
          result = buf.readLong();
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          BigInteger val = new BigInteger(1, bb);
          result = val.longValue();
        }
        break;

      case BIT:
        Byte val = buf.readByte();
        if (length > 1) buf.skip(length - 1);
        return val;

      case FLOAT:
        result = (long) buf.readFloat();
        break;

      case DOUBLE:
        result = (long) buf.readDouble();
        break;

      case OLDDECIMAL:
      case DECIMAL:
      case ENUM:
      case VARCHAR:
      case VARSTRING:
      case STRING:
        String str = buf.readString(length);
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValue();
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(
              String.format("value '%s' (%s) cannot be decoded as Byte", str, column.getType()));
        }
        break;

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (length > 0) {
          byte b = buf.readByte();
          buf.skip(length - 1);
          return b;
        }
        throw new SQLDataException("empty String value cannot be decoded as Byte");

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Byte", column.getType()));
    }

    if ((byte) result != result) {
      throw new SQLDataException("byte overflow");
    }

    return (byte) result;
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Byte value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeAscii(Integer.toString((int) value));
  }

  @Override
  public void encodeBinary(PacketWriter encoder, Context context, Byte value, Calendar cal)
      throws IOException {
    encoder.writeByte(value);
  }

  public DataType getBinaryEncodeType() {
    return DataType.TINYINT;
  }
}
