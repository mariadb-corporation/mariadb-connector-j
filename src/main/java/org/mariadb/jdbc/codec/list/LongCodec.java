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
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class LongCodec implements Codec<Long> {

  public static final LongCodec INSTANCE = new LongCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.OLDDECIMAL,
          DataType.VARCHAR,
          DataType.DECIMAL,
          DataType.ENUM,
          DataType.VARSTRING,
          DataType.STRING,
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.BIGINT,
          DataType.BIT,
          DataType.YEAR);

  public static long parseNotEmpty(ReadableByteBuf buf, int length) {

    boolean negate = false;
    int idx = 1;
    long result = buf.readByte() - 48;

    if (result == -3) { // minus sign
      negate = true;
      idx = 2;
      result = buf.readByte() - 48;
    }

    while (idx++ < length) {
      result = result * 10 + buf.readByte() - 48;
    }

    if (negate) result = -1 * result;
    return result;
  }

  public String className() {
    return Long.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Integer.TYPE) || type.isAssignableFrom(Long.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Long;
  }

  @Override
  public Long decodeText(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    long result;
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case YEAR:
        return parseNotEmpty(buf, length);

      case BIGINT:
        if (column.isSigned()) {
          return parseNotEmpty(buf, length);
        } else {
          BigInteger val = new BigInteger(buf.readAscii(length));
          try {
            return val.longValueExact();
          } catch (ArithmeticException ae) {
            throw new SQLDataException(
                String.format("value '%s' cannot be decoded as Long", val.toString()));
          }
        }

      case BIT:
        result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        return result;

      case DOUBLE:
      case FLOAT:
      case DECIMAL:
        String str2 = buf.readAscii(length);
        try {
          return new BigDecimal(str2).setScale(0, RoundingMode.DOWN).longValue();
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str2));
        }

      case VARCHAR:
      case VARSTRING:
      case STRING:
        String str = buf.readString(length);
        try {
          return new BigInteger(str).longValueExact();
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Long", column.getType()));
    }
  }

  @Override
  public Long decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {

    switch (column.getType()) {
      case BIT:
        long result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        return result;

      case TINYINT:
        if (!column.isSigned()) {
          return (long) buf.readUnsignedByte();
        }
        return (long) buf.readByte();

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return (long) buf.readUnsignedShort();
        }
        return (long) buf.readShort();

      case MEDIUMINT:
        long l = column.isSigned() ? buf.readMedium() : buf.readUnsignedMedium();
        buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
        return l;

      case INTEGER:
        if (!column.isSigned()) {
          return buf.readUnsignedInt();
        }
        return (long) buf.readInt();

      case BIGINT:
        if (column.isSigned()) {
          return buf.readLong();
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          BigInteger val = new BigInteger(1, bb);
          try {
            return val.longValueExact();
          } catch (ArithmeticException ae) {
            throw new SQLDataException(
                String.format("value '%s' cannot be decoded as Long", val.toString()));
          }
        }

      case FLOAT:
        return (long) buf.readFloat();

      case DOUBLE:
        return (long) buf.readDouble();

      case VARSTRING:
      case VARCHAR:
      case STRING:
      case OLDDECIMAL:
      case DECIMAL:
        String str = buf.readString(length);
        try {
          return new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Long", column.getType()));
    }
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    encoder.writeAscii(value.toString());
  }

  @Override
  public void encodeBinary(PacketWriter encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeLong((Long) value);
  }

  public int getBinaryEncodeType() {
    return DataType.BIGINT.get();
  }
}
