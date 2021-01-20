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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.SQLDataException;
import java.util.Calendar;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class TinyIntCodec implements Codec<Byte> {

  public static final TinyIntCodec INSTANCE = new TinyIntCodec();

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return column.getType() == DataType.TINYINT
        && ((type.isPrimitive() && type == Byte.TYPE) || type.isAssignableFrom(Byte.class));
  }

  public boolean canEncode(Object value) {
    return false;
  }

  public String className() {
    return Byte.class.getName();
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
      case YEAR:
      case BIGINT:
        result = LongCodec.parseNotEmpty(buf, length);
        break;

      case BIT:
        result = 0;
        for (int i = 0; i < Math.min(length, 8); i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        if (length > 8) {
          buf.skip(length - 8);
        }
        break;

      case FLOAT:
      case DOUBLE:
      case OLDDECIMAL:
      case VARCHAR:
      case DECIMAL:
      case ENUM:
      case VARSTRING:
      case STRING:
        String str = buf.readString(length);
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).byteValueExact();
          break;
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as byte", str));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as byte", column.getType()));
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
          break;
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          BigInteger val = new BigInteger(1, bb);
          try {
            result = val.intValueExact();
            break;
          } catch (ArithmeticException ae) {
            throw new SQLDataException(
                String.format("value '%s' cannot be decoded as Byte", val.toString()));
          }
        }

      case BIT:
        result = 0;
        for (int i = 0; i < Math.min(length, 8); i++) {
          result += (long) (buf.readByte() & 0xff) << i * 8;
        }
        if (length > 8) {
          buf.skip(length - 8);
        }
        break;

      case FLOAT:
        result = (long) buf.readFloat();
        break;

      case DOUBLE:
        result = (long) buf.readDouble();
        break;

      case OLDDECIMAL:
      case VARCHAR:
      case DECIMAL:
      case ENUM:
      case VARSTRING:
      case STRING:
        String str = buf.readString(length);
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValue();
          break;
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Byte", str));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Byte", column.getType()));
    }

    if ((byte) result != result || (result < 0 && !column.isSigned())) {
      throw new SQLDataException("Byte overflow");
    }

    return (byte) result;
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Byte value, Calendar cal, Long maxLen) {}

  @Override
  public void encodeBinary(PacketWriter encoder, Context context, Byte value, Calendar cal) {}

  public DataType getBinaryEncodeType() {
    return DataType.TINYINT;
  }
}
