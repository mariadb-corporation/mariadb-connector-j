// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.codec.list;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.MariaDbClob;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.util.constants.ServerStatus;

public class ClobCodec implements Codec<Clob> {

  public static final ClobCodec INSTANCE = new ClobCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

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

  @SuppressWarnings("fallthrough")
  private Clob getClob(ReadableByteBuf buf, int length, ColumnDefinitionPacket column)
      throws SQLDataException {
    switch (column.getType()) {
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Clob", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if it has a collation (this is TEXT column)

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
      PacketWriter encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException, SQLException {
    Reader reader = ((Clob) value).getCharacterStream();
    char[] buf = new char[4096];
    int len;
    long remainingLen = maxLength == null ? Long.MAX_VALUE : maxLength;
    encoder.writeByte('\'');
    while (remainingLen > 0 && (len = reader.read(buf)) >= 0) {
      byte[] data =
          new String(buf, 0, (int) Math.min(len, remainingLen)).getBytes(StandardCharsets.UTF_8);
      encoder.writeBytesEscaped(
          data, data.length, (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
      remainingLen -= len;
    }
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, Object value, Calendar cal, Long maxLength)
      throws IOException, SQLException {
    // prefer use of encodeLongData, because length is unknown
    Reader reader = ((Clob) value).getCharacterStream();
    byte[] clobBytes = new byte[4096];
    int pos = 0;
    char[] buf = new char[4096];
    long remainingLen = maxLength == null ? Long.MAX_VALUE : maxLength;
    int len;
    while (remainingLen > 0 && (len = reader.read(buf)) > 0) {
      byte[] data =
          new String(buf, 0, (int) Math.min(len, remainingLen)).getBytes(StandardCharsets.UTF_8);
      if (clobBytes.length - pos < data.length) {
        byte[] newBlobBytes = new byte[clobBytes.length + 65536];
        System.arraycopy(clobBytes, 0, newBlobBytes, 0, pos);
        clobBytes = newBlobBytes;
      }
      System.arraycopy(data, 0, clobBytes, pos, data.length);
      pos += data.length;
      remainingLen -= len;
    }
    encoder.writeLength(pos);
    encoder.writeBytes(clobBytes, 0, pos);
  }

  @Override
  public void encodeLongData(PacketWriter encoder, Clob value, Long maxLength)
      throws IOException, SQLException {
    Reader reader = value.getCharacterStream();
    char[] buf = new char[4096];
    int len;
    long remainingLen = maxLength == null ? Long.MAX_VALUE : maxLength;
    while (remainingLen > 0 && (len = reader.read(buf)) > 0) {
      byte[] data =
          new String(buf, 0, (int) Math.min(len, remainingLen)).getBytes(StandardCharsets.UTF_8);
      encoder.writeBytes(data, 0, data.length);
      remainingLen -= len;
    }
  }

  @Override
  public byte[] encodeData(Clob value, Long maxLength) throws IOException, SQLException {
    ByteArrayOutputStream bb = new ByteArrayOutputStream();
    Reader reader = value.getCharacterStream();
    char[] buf = new char[4096];
    int len;
    long remainingLen = maxLength == null ? Long.MAX_VALUE : maxLength;
    while (remainingLen > 0 && (len = reader.read(buf)) > 0) {
      byte[] data =
          new String(buf, 0, (int) Math.min(len, remainingLen)).getBytes(StandardCharsets.UTF_8);
      bb.write(data, 0, data.length);
      remainingLen -= len;
    }
    return bb.toByteArray();
  }

  public boolean canEncodeLongData() {
    return true;
  }

  public int getBinaryEncodeType() {
    return DataType.VARSTRING.get();
  }
}
