// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.util.constants.ServerStatus;

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

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Blob.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Blob && !(value instanceof Clob);
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Blob decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException, IOException {
    switch (column.getType()) {
      case STRING:
      case VARCHAR:
      case VARSTRING:
      case BIT:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case BLOB:
      case GEOMETRY:
        return buf.readBlob(length.get());

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Blob", column.getType()));
    }
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Blob decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException, IOException {
    switch (column.getType()) {
      case STRING:
      case VARCHAR:
      case VARSTRING:
      case BIT:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case BLOB:
      case GEOMETRY:
        return buf.readBlob(length.get());

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Blob", column.getType()));
    }
  }

  @Override
  public void encodeText(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
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
  public void encodeBinary(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
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
