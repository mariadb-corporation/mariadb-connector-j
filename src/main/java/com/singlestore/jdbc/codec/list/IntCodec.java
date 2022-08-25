// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.codec.list;

import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.context.Context;
import com.singlestore.jdbc.client.socket.PacketWriter;
import com.singlestore.jdbc.codec.Codec;
import com.singlestore.jdbc.codec.DataType;
import com.singlestore.jdbc.message.server.ColumnDefinitionPacket;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;

public class IntCodec implements Codec<Integer> {

  public static final IntCodec INSTANCE = new IntCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.OLDDECIMAL,
          DataType.VARCHAR,
          DataType.DECIMAL,
          DataType.ENUM,
          DataType.CHAR,
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INT,
          DataType.BIGINT,
          DataType.BIT,
          DataType.YEAR,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Integer.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Integer.TYPE) || type.isAssignableFrom(Integer.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Integer;
  }

  @Override
  public Integer decodeText(
      final ReadableByteBuf buffer,
      final int length,
      final ColumnDefinitionPacket column,
      final Calendar cal)
      throws SQLDataException {
    return decodeTextInt(buffer, length, column);
  }

  @SuppressWarnings("fallthrough")
  public int decodeTextInt(
      final ReadableByteBuf buf, final int length, final ColumnDefinitionPacket column)
      throws SQLDataException {
    long result;
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case YEAR:
        return (int) LongCodec.parseNotEmpty(buf, length);

      case INT:
        result = LongCodec.parseNotEmpty(buf, length);
        break;

      case BIGINT:
        result = LongCodec.parseNotEmpty(buf, length);
        if (result < 0 & !column.isSigned()) {
          throw new SQLDataException("int overflow");
        }
        break;

      case BIT:
        result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        break;

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Integer", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case FLOAT:
      case DOUBLE:
      case OLDDECIMAL:
      case VARCHAR:
      case DECIMAL:
      case ENUM:
      case CHAR:
        String str = buf.readString(length);
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
          break;
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Integer", str));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Integer", column.getType()));
    }

    int res = (int) result;
    if (res != result) {
      throw new SQLDataException("integer overflow");
    }
    return res;
  }

  @Override
  public Integer decodeBinary(
      final ReadableByteBuf buffer,
      final int length,
      final ColumnDefinitionPacket column,
      final Calendar cal)
      throws SQLDataException {
    return decodeBinaryInt(buffer, length, column);
  }

  @SuppressWarnings("fallthrough")
  public int decodeBinaryInt(ReadableByteBuf buf, int length, ColumnDefinitionPacket column)
      throws SQLDataException {
    long result;
    switch (column.getType()) {
      case TINYINT:
        return (column.isSigned() ? buf.readByte() : buf.readUnsignedByte());

      case YEAR:
      case SMALLINT:
        return column.isSigned() ? buf.readShort() : buf.readUnsignedShort();

      case MEDIUMINT:
        int res = column.isSigned() ? buf.readMedium() : buf.readUnsignedMedium();
        buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
        return res;

      case INT:
        if (column.isSigned()) {
          return buf.readInt();
        }
        result = buf.readUnsignedInt();
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
            return val.intValueExact();
          } catch (ArithmeticException ae) {
            throw new SQLDataException(
                String.format("value '%s' cannot be decoded as Integer", val));
          }
        }

      case BIT:
        result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        break;

      case FLOAT:
        result = (long) buf.readFloat();
        break;

      case DOUBLE:
        result = (long) buf.readDouble();
        break;

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Integer", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case OLDDECIMAL:
      case DECIMAL:
      case ENUM:
      case VARCHAR:
      case CHAR:
        String str = buf.readString(length);
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
          break;
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Integer", str));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Integer", column.getType()));
    }

    int res = (int) result;
    if (res != result) {
      throw new SQLDataException("integer overflow");
    }

    return res;
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    encoder.writeAscii(value.toString());
  }

  @Override
  public void encodeBinary(PacketWriter encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeInt(((Integer) value).intValue());
  }

  public int getBinaryEncodeType() {
    return DataType.INT.get();
  }
}
