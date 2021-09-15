// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.codec.list;

import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.context.Context;
import com.singlestore.jdbc.client.socket.PacketWriter;
import com.singlestore.jdbc.codec.Codec;
import com.singlestore.jdbc.codec.DataType;
import com.singlestore.jdbc.message.server.ColumnDefinitionPacket;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;

public class BooleanCodec implements Codec<Boolean> {

  public static final BooleanCodec INSTANCE = new BooleanCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING,
          DataType.BIGINT,
          DataType.INTEGER,
          DataType.MEDIUMINT,
          DataType.SMALLINT,
          DataType.YEAR,
          DataType.TINYINT,
          DataType.DECIMAL,
          DataType.OLDDECIMAL,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.BIT,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Boolean.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Boolean.TYPE) || type.isAssignableFrom(Boolean.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Boolean;
  }

  public Boolean decodeText(
      final ReadableByteBuf buffer,
      final int length,
      final ColumnDefinitionPacket column,
      final Calendar cal)
      throws SQLDataException {
    return decodeTextBoolean(buffer, length, column);
  }

  @SuppressWarnings("fallthrough")
  public boolean decodeTextBoolean(ReadableByteBuf buf, int length, ColumnDefinitionPacket column)
      throws SQLDataException {
    switch (column.getType()) {
      case BIT:
        return ByteCodec.parseBit(buf, length) != 0;

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Boolean", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case VARCHAR:
      case VARSTRING:
      case STRING:
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case YEAR:
        String s = buf.readAscii(length);
        return !"0".equals(s);

      case DECIMAL:
      case OLDDECIMAL:
      case FLOAT:
      case DOUBLE:
        return new BigDecimal(buf.readAscii(length)).intValue() != 0;

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Boolean", column.getType()));
    }
  }

  public Boolean decodeBinary(
      final ReadableByteBuf buffer,
      final int length,
      final ColumnDefinitionPacket column,
      final Calendar cal)
      throws SQLDataException {
    return decodeBinaryBoolean(buffer, length, column);
  }

  @SuppressWarnings("fallthrough")
  public boolean decodeBinaryBoolean(ReadableByteBuf buf, int length, ColumnDefinitionPacket column)
      throws SQLDataException {
    switch (column.getType()) {
      case BIT:
        return ByteCodec.parseBit(buf, length) != 0;

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Boolean", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case VARCHAR:
      case VARSTRING:
      case STRING:
        return !"0".equals(buf.readAscii(length));

      case DECIMAL:
      case OLDDECIMAL:
        return new BigDecimal(buf.readAscii(length)).intValue() != 0;

      case FLOAT:
        return ((int) buf.readFloat()) != 0;

      case DOUBLE:
        return ((int) buf.readDouble()) != 0;

      case TINYINT:
        return buf.readByte() != 0;

      case YEAR:
      case SMALLINT:
        return buf.readShort() != 0;

      case MEDIUMINT:
      case INTEGER:
        return buf.readInt() != 0;
      case BIGINT:
        return buf.readLong() != 0;

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Boolean", column.getType()));
    }
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeAscii(((Boolean) value) ? "1" : "0");
  }

  @Override
  public void encodeBinary(PacketWriter encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeByte(((Boolean) value) ? 1 : 0);
  }

  public int getBinaryEncodeType() {
    return DataType.TINYINT.get();
  }
}
