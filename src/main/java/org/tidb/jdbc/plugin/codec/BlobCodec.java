// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.plugin.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.EnumSet;
import org.tidb.jdbc.MariaDbBlob;
import org.tidb.jdbc.client.Column;
import org.tidb.jdbc.client.Context;
import org.tidb.jdbc.client.DataType;
import org.tidb.jdbc.client.ReadableByteBuf;
import org.tidb.jdbc.client.socket.Writer;
import org.tidb.jdbc.plugin.Codec;
import org.tidb.jdbc.util.constants.ServerStatus;

/** Blob codec */
public class BlobCodec implements Codec<Blob> {

  /** default instance */
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

  public boolean canDecode(Column column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Blob.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Blob && !(value instanceof Clob);
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Blob decodeText(ReadableByteBuf buf, int length, Column column, Calendar cal)
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
        if (!column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format(
                  "Data type %s (not binary) cannot be decoded as Blob", column.getType()));
        }
        return buf.readBlob(length);

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Blob", column.getType()));
    }
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Blob decodeBinary(ReadableByteBuf buf, int length, Column column, Calendar cal)
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
        if (!column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format(
                  "Data type %s (not binary) cannot be decoded as Blob", column.getType()));
        }
        buf.skip(length);
        return new MariaDbBlob(buf.buf(), buf.pos() - length, length);

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Blob", column.getType()));
    }
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException, SQLException {
    encoder.writeBytes(ByteArrayCodec.BINARY_PREFIX);
    byte[] array = new byte[4096];
    InputStream is = ((Blob) value).getBinaryStream();
    int len;

    if (maxLength == null) {
      while ((len = is.read(array)) > 0) {
        encoder.writeBytesEscaped(
            array, len, (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
      }
    } else {
      long maxLen = maxLength;
      while (maxLen > 0 && (len = is.read(array)) > 0) {
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
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException, SQLException {
    long length;
    InputStream is = ((Blob) value).getBinaryStream();
    try {
      length = ((Blob) value).length();
      if (maxLength != null) length = Math.min(maxLength, length);

      // if not have thrown an error
      encoder.writeLength(length);
      byte[] array = new byte[4096];
      int len;
      long remainingLen = length;
      while ((len = is.read(array)) > 0) {
        encoder.writeBytes(array, 0, Math.min((int) remainingLen, len));
        remainingLen -= len;
        if (remainingLen < 0) break;
      }

    } catch (SQLException sqle) {
      byte[] val = encode(is, maxLength);
      encoder.writeLength(val.length);
      encoder.writeBytes(val, 0, val.length);
    }
  }

  @Override
  public void encodeLongData(Writer encoder, Blob value, Long maxLength)
      throws IOException, SQLException {
    byte[] array = new byte[4096];
    InputStream is = value.getBinaryStream();

    if (maxLength == null) {
      int len;
      while ((len = is.read(array)) > 0) {
        encoder.writeBytes(array, 0, len);
      }
    } else {
      long maxLen = maxLength;
      int len;
      while (maxLen > 0 && (len = is.read(array)) > 0) {
        encoder.writeBytes(array, 0, Math.min(len, (int) maxLen));
        maxLen -= len;
      }
    }
  }

  @Override
  public byte[] encodeData(Blob value, Long maxLength) throws IOException, SQLException {
    return encode(value.getBinaryStream(), maxLength);
  }

  private byte[] encode(InputStream is, Long maxLength) throws IOException {
    ByteArrayOutputStream bb = new ByteArrayOutputStream();
    byte[] array = new byte[4096];
    if (maxLength == null) {
      int len;
      while ((len = is.read(array)) > 0) {
        bb.write(array, 0, len);
      }
    } else {
      long maxLen = maxLength;
      int len;
      while (maxLen > 0 && (len = is.read(array)) > 0) {
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
