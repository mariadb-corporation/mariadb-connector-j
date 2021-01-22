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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.MariaDbClob;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.util.constants.ServerStatus;

public class ClobCodec implements Codec<Clob> {

  public static final ClobCodec INSTANCE = new ClobCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.VARCHAR, DataType.VARSTRING, DataType.STRING);

  public String className() {
    return Clob.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && (type.isAssignableFrom(Clob.class) || type.isAssignableFrom(NClob.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Clob;
  }

  @Override
  public Clob decodeText(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    return getClob(buf, length, column);
  }

  private Clob getClob(ReadableByteBuf buf, int length, ColumnDefinitionPacket column)
      throws SQLDataException {
    switch (column.getType()) {
      case STRING:
      case VARCHAR:
      case VARSTRING:
        Clob clob = new MariaDbClob(buf.buf(), buf.pos(), length);
        buf.skip(length);
        return clob;

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Clob", column.getType()));
    }
  }

  @Override
  public Clob decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    return getClob(buf, length, column);
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException, SQLException {
    Reader reader = ((Clob)value).getCharacterStream();
    char[] buf = new char[4096];
    int len;
    encoder.writeByte('\'');
    while ((len = reader.read(buf)) >= 0) {
      byte[] data = new String(buf, 0, len).getBytes(StandardCharsets.UTF_8);
      encoder.writeBytesEscaped(
          data, data.length, (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
    }
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, Context context, Object value, Calendar cal)
      throws IOException, SQLException {
    // prefer use of encodeLongData, because length is unknown
    Reader reader = ((Clob)value).getCharacterStream();
    byte[] clobBytes = new byte[4096];
    int pos = 0;
    char[] buf = new char[4096];

    int len;
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
    encoder.writeLength(pos);
    encoder.writeBytes(clobBytes, 0, pos);
  }

  @Override
  public void encodeLongData(PacketWriter encoder, Context context, Clob value, Long length)
      throws IOException, SQLException {
    Reader reader = value.getCharacterStream();
    char[] buf = new char[4096];
    int len;
    while ((len = reader.read(buf)) >= 0) {
      byte[] data = new String(buf, 0, len).getBytes(StandardCharsets.UTF_8);
      encoder.writeBytes(data, 0, data.length);
    }
  }

  @Override
  public byte[] encodeLongDataReturning(
      PacketWriter encoder, Context context, Clob value, Long length)
      throws IOException, SQLException {
    ByteArrayOutputStream bb = new ByteArrayOutputStream();
    Reader reader = value.getCharacterStream();
    char[] buf = new char[4096];
    int len;
    while ((len = reader.read(buf)) >= 0) {
      byte[] data = new String(buf, 0, len).getBytes(StandardCharsets.UTF_8);
      bb.write(data, 0, data.length);
    }
    byte[] val = bb.toByteArray();
    encoder.writeBytes(val);
    return val;
  }

  public boolean canEncodeLongData() {
    return true;
  }

  public int getBinaryEncodeType() {
    return DataType.VARSTRING.get();
  }
}
