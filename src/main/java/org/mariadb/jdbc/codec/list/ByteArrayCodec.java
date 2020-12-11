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
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.util.constants.ServerStatus;

public class ByteArrayCodec implements Codec<byte[]> {

  public static final byte[] BINARY_PREFIX = {'_', 'b', 'i', 'n', 'a', 'r', 'y', ' ', '\''};

  public static final ByteArrayCodec INSTANCE = new ByteArrayCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.BIT,
          DataType.GEOMETRY,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING);

  public String className() {
    return byte[].class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Byte.TYPE && type.isArray())
            || type.isAssignableFrom(byte[].class));
  }

  @Override
  public byte[] decodeText(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    switch (column.getType()) {
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case STRING:
      case VARSTRING:
      case VARCHAR:
        byte[] arr = new byte[length];
        buf.readBytes(arr);
        return arr;

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as byte[]", column.getType()));
    }
  }

  @Override
  public byte[] decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    switch (column.getType()) {
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case STRING:
      case VARSTRING:
      case VARCHAR:
        byte[] arr = new byte[length];
        buf.readBytes(arr);
        return arr;

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as byte[]", column.getType()));
    }
  }

  public boolean canEncode(Object value) {
    return value instanceof byte[];
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, byte[] value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeBytes(BINARY_PREFIX);
    encoder.writeBytesEscaped(
        value,
        maxLength == null ? value.length : Math.min(value.length, maxLength.intValue()),
        (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, Context context, byte[] value, Calendar cal)
      throws IOException {
    encoder.writeLength(value.length);
    encoder.writeBytes(value);
  }

  @Override
  public void encodeLongData(PacketWriter encoder, Context context, byte[] value, Long length)
      throws IOException {
    if (length == null) {
      encoder.writeBytes(value);
    } else {
      encoder.writeBytes(value, 0, length.intValue());
    }
  }

  @Override
  public byte[] encodeLongDataReturning(
      PacketWriter encoder, Context context, byte[] value, Long length) throws IOException {
    if (length == null || length.intValue() >= value.length) {
      encoder.writeBytes(value);
      return value;
    } else {
      encoder.writeBytes(value, 0, length.intValue());
      byte[] bb = new byte[length.intValue()];
      System.arraycopy(value, 0, bb, 0, length.intValue());
      return bb;
    }
  }

  public boolean canEncodeLongData() {
    return true;
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }
}
