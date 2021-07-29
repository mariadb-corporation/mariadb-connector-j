// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.codec.list;

import java.io.*;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.util.constants.ServerStatus;

public class StreamCodec implements Codec<InputStream> {

  public static final StreamCodec INSTANCE = new StreamCodec();

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
      PacketWriter encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    encoder.writeBytes(ByteArrayCodec.BINARY_PREFIX);
    byte[] array = new byte[4096];
    int len;
    InputStream stream = (InputStream) value;

    if (maxLen == null) {
      while ((len = stream.read(array)) > 0) {
        encoder.writeBytesEscaped(
            array, len, (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
      }
    } else {
      while ((len = stream.read(array)) > 0 && maxLen > 0) {
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
  public void encodeBinary(PacketWriter encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    // length is not known
    byte[] blobBytes = new byte[4096];
    int pos = 0;
    byte[] array = new byte[4096];
    InputStream stream = (InputStream) value;

    int len;
    if (maxLength == null) {
      while ((len = stream.read(array)) > 0) {
        if (blobBytes.length - pos < len) {
          byte[] newBlobBytes = new byte[blobBytes.length + 65536];
          System.arraycopy(blobBytes, 0, newBlobBytes, 0, blobBytes.length);
          blobBytes = newBlobBytes;
        }
        System.arraycopy(array, 0, blobBytes, pos, len);
        pos += len;
      }
    } else {
      long remainingLen = maxLength;
      while ((len = stream.read(array)) > 0 && remainingLen > 0) {
        len = Math.min((int) remainingLen, len);
        if (blobBytes.length - pos < len) {
          byte[] newBlobBytes = new byte[blobBytes.length + 65536];
          System.arraycopy(blobBytes, 0, newBlobBytes, 0, blobBytes.length);
          blobBytes = newBlobBytes;
        }
        System.arraycopy(array, 0, blobBytes, pos, len);
        pos += len;
        remainingLen -= len;
      }
    }
    encoder.writeLength(pos);
    encoder.writeBytes(blobBytes, 0, pos);
  }

  @Override
  public void encodeLongData(PacketWriter encoder, InputStream value, Long maxLength)
      throws IOException {
    byte[] array = new byte[4096];
    int len;
    if (maxLength == null) {
      while ((len = value.read(array)) > 0) {
        encoder.writeBytes(array, 0, len);
      }
    } else {
      long maxLen = maxLength;
      while ((len = value.read(array)) > 0 && maxLen > 0) {
        encoder.writeBytes(array, 0, Math.min(len, (int) maxLen));
        maxLen -= len;
      }
    }
  }

  @Override
  public byte[] encodeData(InputStream value, Long maxLength) throws IOException {
    ByteArrayOutputStream bb = new ByteArrayOutputStream();
    byte[] array = new byte[4096];
    int len;
    if (maxLength == null) {
      while ((len = value.read(array)) > 0) {
        bb.write(array, 0, len);
      }
    } else {
      long maxLen = maxLength;
      while ((len = value.read(array)) > 0 && maxLen > 0) {
        bb.write(array, 0, Math.min(len, (int) maxLen));
        maxLen -= len;
      }
    }
    return bb.toByteArray();
  }

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }

  public boolean canEncodeLongData() {
    return true;
  }
}
