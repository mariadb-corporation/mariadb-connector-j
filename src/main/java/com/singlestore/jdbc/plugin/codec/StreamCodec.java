// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.plugin.codec;

import com.singlestore.jdbc.client.ColumnDecoder;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.util.MutableInt;
import com.singlestore.jdbc.plugin.Codec;
import com.singlestore.jdbc.util.constants.ServerStatus;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.EnumSet;

public class StreamCodec implements Codec<InputStream> {

  public static final StreamCodec INSTANCE = new StreamCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.VARCHAR,
          DataType.CHAR,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return InputStream.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(InputStream.class);
  }

  @Override
  public InputStream decodeText(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    switch (column.getType()) {
      case CHAR:
      case VARCHAR:
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        ByteArrayInputStream is = new ByteArrayInputStream(buf.buf(), buf.pos(), length.get());
        buf.skip(length.get());
        return is;
      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Stream", column.getType()));
    }
  }

  @Override
  public InputStream decodeBinary(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    switch (column.getType()) {
      case CHAR:
      case VARCHAR:
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        ByteArrayInputStream is = new ByteArrayInputStream(buf.buf(), buf.pos(), length.get());
        buf.skip(length.get());
        return is;
      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Stream", column.getType()));
    }
  }

  public boolean canEncode(Object value) {
    return value instanceof InputStream;
  }

  @Override
  public int getApproximateTextProtocolLength(Object value) throws SQLException {
    return -1;
  }

  @Override
  public void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long maxLen)
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
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
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
      long remainingLen = maxLength.longValue();
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
  public void encodeLongData(Writer encoder, InputStream value, Long maxLength) throws IOException {
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

  @Override
  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
