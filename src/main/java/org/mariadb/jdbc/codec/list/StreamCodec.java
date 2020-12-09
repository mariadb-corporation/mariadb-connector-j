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

import java.io.*;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.PacketWriter;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.util.constants.ServerStatus;

public class StreamCodec implements Codec<InputStream> {

  public static final StreamCodec INSTANCE = new StreamCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING);

  public String className() {
    return InputStream.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(InputStream.class);
  }

  @Override
  public InputStream decodeText(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    switch (column.getType()) {
      case STRING:
      case VARCHAR:
      case VARSTRING:
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        ByteArrayInputStream is = new ByteArrayInputStream(buf.buf(), buf.pos(), length);
        buf.skip(length);
        return is;
      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Stream", column.getType()));
    }
  }

  @Override
  public InputStream decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    switch (column.getType()) {
      case STRING:
      case VARCHAR:
      case VARSTRING:
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        ByteArrayInputStream is = new ByteArrayInputStream(buf.buf(), buf.pos(), length);
        buf.skip(length);
        return is;
      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Stream", column.getType()));
    }
  }

  public boolean canEncode(Object value) {
    return value instanceof InputStream;
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, InputStream value, Calendar cal, Long maxLen)
      throws IOException {
    encoder.writeBytes(ByteArrayCodec.BINARY_PREFIX);
    byte[] array = new byte[4096];
    int len;

    if (maxLen == null) {
      while ((len = value.read(array)) > 0) {
        encoder.writeBytesEscaped(
            array, len, (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
      }
    } else {
      while ((len = value.read(array)) > 0 && maxLen > 0) {
        encoder.writeBytesEscaped(
            array,
            Math.min(len, maxLen.intValue()),
            (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
        maxLen -= len;
      }
    }
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, Context context, InputStream value, Calendar cal)
      throws IOException {
    // length is not known
    byte[] blobBytes = new byte[4096];
    int pos = 0;
    byte[] array = new byte[4096];

    int len;
    while ((len = value.read(array)) > 0) {
      if (blobBytes.length - (pos + 1) < len) {
        byte[] newBlobBytes = new byte[blobBytes.length + 65536];
        System.arraycopy(blobBytes, 0, newBlobBytes, 0, blobBytes.length);
        pos = blobBytes.length;
        blobBytes = newBlobBytes;
      }
      System.arraycopy(array, 0, blobBytes, pos, len);
      pos += len;
    }
    encoder.writeLength(pos);
    encoder.writeBytes(blobBytes, 0, pos);
  }

  @Override
  public void encodeLongData(PacketWriter encoder, Context context, InputStream value, Long length)
      throws IOException {
    byte[] array = new byte[4096];
    int len;
    if (length == null) {
      while ((len = value.read(array)) > 0) {
        encoder.writeBytes(array, 0, len);
      }
    } else {
      long maxLen = length;
      while ((len = value.read(array)) > 0 && maxLen > 0) {
        encoder.writeBytes(array, 0, Math.min(len, (int) maxLen));
        maxLen -= len;
      }
    }
  }

  @Override
  public byte[] encodeLongDataReturning(
      PacketWriter encoder, Context context, InputStream value, Long length)
      throws IOException, SQLException {
    ByteArrayOutputStream bb = new ByteArrayOutputStream();
    byte[] array = new byte[4096];
    int len;
    if (length == null) {
      while ((len = value.read(array)) > 0) {
        bb.write(array, 0, len);
      }
    } else {
      long maxLen = length;
      while ((len = value.read(array)) > 0 && maxLen > 0) {
        bb.write(array, 0, Math.min(len, (int) maxLen));
        maxLen -= len;
      }
    }
    byte[] val = bb.toByteArray();
    encoder.writeBytes(val);
    return val;
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }

  public boolean canEncodeLongData() {
    return true;
  }
}
