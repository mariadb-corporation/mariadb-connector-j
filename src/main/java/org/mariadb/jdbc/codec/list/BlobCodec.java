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
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.util.constants.ServerStatus;

public class BlobCodec implements Codec<Blob> {

  public static final BlobCodec INSTANCE = new BlobCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BIT,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.STRING,
          DataType.VARSTRING,
          DataType.VARCHAR);

  public String className() {
    return Blob.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Blob.class);
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Blob decodeText(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    switch (column.getType()) {
      case STRING:
      case VARCHAR:
      case VARSTRING:
        if (!column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format(
                  "Data type %s (not binary) cannot be decoded as Blob", column.getType()));
        }
      case BIT:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case BLOB:
      case GEOMETRY:
        return buf.readBlob(length);

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Blob", column.getType()));
    }
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Blob decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    switch (column.getType()) {
      case STRING:
      case VARCHAR:
      case VARSTRING:
        if (!column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format(
                  "Data type %s (not binary) cannot be decoded as Blob", column.getType()));
        }
      case BIT:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case BLOB:
      case GEOMETRY:
        buf.skip(length);
        return new MariaDbBlob(buf.buf(), buf.pos() - length, length);

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Blob", column.getType()));
    }
  }

  public boolean canEncode(Object value) {
    return value instanceof Blob;
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException, SQLException {
    encoder.writeBytes(ByteArrayCodec.BINARY_PREFIX);
    byte[] array = new byte[4096];
    InputStream is = ((Blob)value).getBinaryStream();
    int len;

    if (maxLength == null) {
      while ((len = is.read(array)) > 0) {
        encoder.writeBytesEscaped(
            array, len, (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
      }
    } else {
      long maxLen = maxLength;
      while ((len = is.read(array)) > 0 && maxLen > 0) {
        encoder.writeBytesEscaped(
            array,
            Math.min(len, (int) maxLen),
            (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
        maxLen -= len;
      }
    }
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(PacketWriter encoder, Context context, Object value, Calendar cal)
      throws IOException, SQLException {
    long length;
    InputStream is = ((Blob)value).getBinaryStream();
    try {
      length = ((Blob)value).length();

      // if not have thrown an error
      encoder.writeLength(length);
      byte[] array = new byte[4096];
      int len;
      while ((len = is.read(array)) > 0) {
        encoder.writeBytes(array, 0, len);
      }

    } catch (SQLException sqle) {

      // length is not known
      byte[] blobBytes = new byte[4096];
      int pos = 0;
      byte[] array = new byte[4096];

      int len;
      while ((len = is.read(array)) > 0) {
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
  }

  @Override
  public void encodeLongData(PacketWriter encoder, Context context, Blob value, Long maxLength)
      throws IOException, SQLException {
    if (maxLength == null) {
      byte[] array = new byte[4096];
      InputStream is = value.getBinaryStream();
      int len;
      while ((len = is.read(array)) > 0) {
        encoder.writeBytes(array, 0, len);
      }
    } else {
      long maxLen = maxLength;
      byte[] array = new byte[4096];
      InputStream is = value.getBinaryStream();
      int len;
      while ((len = is.read(array)) > 0 && maxLen > 0) {
        encoder.writeBytes(array, 0, Math.min(len, (int) maxLen));
        maxLen -= len;
      }
    }
  }

  @Override
  public byte[] encodeLongDataReturning(
      PacketWriter encoder, Context context, Blob value, Long maxLength)
      throws IOException, SQLException {
    ByteArrayOutputStream bb = new ByteArrayOutputStream();
    if (maxLength == null) {
      byte[] array = new byte[4096];
      InputStream is = value.getBinaryStream();
      int len;
      while ((len = is.read(array)) > 0) {
        bb.write(array, 0, len);
      }
    } else {
      long maxLen = maxLength;
      byte[] array = new byte[4096];
      InputStream is = value.getBinaryStream();
      int len;
      while ((len = is.read(array)) > 0 && maxLen > 0) {
        bb.write(array, 0, Math.min(len, (int) maxLen));
        maxLen -= len;
      }
    }
    byte[] val = bb.toByteArray();
    encoder.writeBytes(val);
    return val;
  }

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }

  public boolean canEncodeLongData() {
    return true;
  }
}
