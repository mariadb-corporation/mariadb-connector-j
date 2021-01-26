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
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class DoubleCodec implements Codec<Double> {

  public static final DoubleCodec INSTANCE = new DoubleCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.BIGINT,
          DataType.YEAR,
          DataType.OLDDECIMAL,
          DataType.DECIMAL,
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Double.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Double.TYPE) || type.isAssignableFrom(Double.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Double;
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Double decodeText(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case OLDDECIMAL:
      case DECIMAL:
        return Double.valueOf(buf.readAscii(length));

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Double", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case VARCHAR:
      case VARSTRING:
      case STRING:
        String str2 = buf.readString(length);
        try {
          return Double.valueOf(str2);
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Double", str2));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Double", column.getType()));
    }
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Double decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    switch (column.getType()) {
      case TINYINT:
        if (!column.isSigned()) {
          return (double) buf.readUnsignedByte();
        }
        return (double) buf.readByte();

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return (double) buf.readUnsignedShort();
        }
        return (double) buf.readShort();

      case MEDIUMINT:
        double d;
        d = column.isSigned() ? buf.readMedium() : buf.readUnsignedMedium();
        buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
        return d;

      case INTEGER:
        if (!column.isSigned()) {
          return (double) buf.readUnsignedInt();
        }
        return (double) buf.readInt();

      case BIGINT:
        if (column.isSigned()) {
          return (double) buf.readLong();
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          return new BigInteger(1, bb).doubleValue();
        }

      case FLOAT:
        return (double) buf.readFloat();

      case DOUBLE:
        return buf.readDouble();

      case OLDDECIMAL:
      case DECIMAL:
        return new BigDecimal(buf.readAscii(length)).doubleValue();

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Double", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case VARCHAR:
      case VARSTRING:
      case STRING:
        String str2 = buf.readString(length);
        try {
          return Double.valueOf(str2);
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Double", str2));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Double", column.getType()));
    }
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    encoder.writeAscii(value.toString());
  }

  @Override
  public void encodeBinary(
      PacketWriter encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeDouble((Double) value);
  }

  public int getBinaryEncodeType() {
    return DataType.DOUBLE.get();
  }
}
