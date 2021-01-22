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
import java.nio.charset.StandardCharsets;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.util.constants.ServerStatus;

public class ReaderCodec implements Codec<Reader> {

  public static final ReaderCodec INSTANCE = new ReaderCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.STRING, DataType.VARCHAR, DataType.VARSTRING);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Reader.class);
  }

  public String className() {
    return Reader.class.getName();
  }

  @Override
  public Reader decodeText(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    switch (column.getType()) {
      case STRING:
      case VARCHAR:
      case VARSTRING:
        return new StringReader(buf.readString(length));

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Reader", column.getType()));
    }
  }

  @Override
  public Reader decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    return decodeText(buf, length, column, cal);
  }

  public boolean canEncode(Object value) {
    return value instanceof Reader;
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Object val, Calendar cal, Long maxLen)
      throws IOException, SQLException {
    Reader reader = (Reader) val;
    encoder.writeByte('\'');
    char[] buf = new char[4096];
    int len;
    if (maxLen == null) {
      while ((len = reader.read(buf)) >= 0) {
        byte[] data = new String(buf, 0, len).getBytes(StandardCharsets.UTF_8);
        encoder.writeBytesEscaped(
            data,
            data.length,
            (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
      }
    } else {
      while ((len = reader.read(buf)) >= 0) {
        byte[] data =
            new String(buf, 0, Math.min(len, maxLen.intValue())).getBytes(StandardCharsets.UTF_8);
        maxLen -= len;
        encoder.writeBytesEscaped(
            data,
            data.length,
            (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
      }
    }
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, Context context, Object val, Calendar cal, Long maxLength)
      throws IOException, SQLException {
    // prefer use of encodeLongData, because length is unknown
    byte[] clobBytes = new byte[4096];
    int pos = 0;
    char[] buf = new char[4096];
    Reader reader = (Reader) val;
    int len;
    if (maxLength == null) {
      while ((len = reader.read(buf)) > 0) {
        byte[] data = new String(buf, 0, len).getBytes(StandardCharsets.UTF_8);
        if (clobBytes.length - (pos + 1) < data.length) {
          byte[] newBlobBytes = new byte[clobBytes.length + 65536];
          System.arraycopy(clobBytes, 0, newBlobBytes, 0, clobBytes.length);
          pos = clobBytes.length;
          clobBytes = newBlobBytes;
        }
        System.arraycopy(data, 0, clobBytes, pos, data.length);
        pos += len;
      }
    } else {
      long maxLen = maxLength.longValue();
      while ((len = reader.read(buf)) > 0) {
        if (len < maxLen) {
          maxLen -= len;
        } else {
          len = (int)maxLen;
          maxLen = 0L;
        }

        byte[] data = new String(buf, 0, len).getBytes(StandardCharsets.UTF_8);
        if (clobBytes.length - (pos + 1) < data.length) {
          byte[] newBlobBytes = new byte[clobBytes.length + 65536];
          System.arraycopy(clobBytes, 0, newBlobBytes, 0, clobBytes.length);
          pos = clobBytes.length;
          clobBytes = newBlobBytes;
        }
        System.arraycopy(data, 0, clobBytes, pos, data.length);
        pos += len;
        if (maxLen == 0L) break;
      }
    }
    encoder.writeLength(pos);
    encoder.writeBytes(clobBytes, 0, pos);
  }

  @Override
  public void encodeLongData(PacketWriter encoder, Context context, Reader reader, Long length)
      throws IOException {
    char[] buf = new char[4096];
    int len;
    if (length == null) {
      while ((len = reader.read(buf)) >= 0) {
        byte[] data = new String(buf, 0, len).getBytes(StandardCharsets.UTF_8);
        encoder.writeBytes(data, 0, data.length);
      }
    } else {
      long maxLen = length;
      while ((len = reader.read(buf)) >= 0 && maxLen > 0) {
        byte[] data =
            new String(buf, 0, Math.min(len, (int) maxLen)).getBytes(StandardCharsets.UTF_8);
        maxLen -= len;
        encoder.writeBytes(data, 0, data.length);
      }
    }
  }

  @Override
  public byte[] encodeLongDataReturning(
      PacketWriter encoder, Context context, Reader reader, Long length) throws IOException {
    ByteArrayOutputStream bb = new ByteArrayOutputStream();
    char[] buf = new char[4096];
    int len;
    if (length == null) {
      while ((len = reader.read(buf)) >= 0) {
        byte[] data = new String(buf, 0, len).getBytes(StandardCharsets.UTF_8);
        bb.write(data, 0, data.length);
      }
    } else {
      long maxLen = length;
      while ((len = reader.read(buf)) >= 0 && maxLen > 0) {
        byte[] data =
            new String(buf, 0, Math.min(len, (int) maxLen)).getBytes(StandardCharsets.UTF_8);
        maxLen -= len;
        bb.write(data, 0, data.length);
      }
    }
    byte[] val = bb.toByteArray();
    encoder.writeBytes(val);
    return val;
  }

  public int getBinaryEncodeType() {
    return DataType.VARSTRING.get();
  }

  public boolean canEncodeLongData() {
    return true;
  }
}
