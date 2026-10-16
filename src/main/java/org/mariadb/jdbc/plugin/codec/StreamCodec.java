// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.plugin.codec;

import java.io.*;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.util.constants.ServerStatus;

/** InputStream codec */
public class StreamCodec implements Codec<InputStream> {

  /** default instance */
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

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(InputStream.class);
  }

  @Override
  public InputStream decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    switch (column.getType()) {
      case STRING:
      case VARCHAR:
      case VARSTRING:
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
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    switch (column.getType()) {
      case STRING:
      case VARCHAR:
      case VARSTRING:
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
  public void encodeText(
      Writer encoder, Context context, InputStream value, Calendar cal, Long maxLen)
      throws IOException {
    encoder.writeBytes(ByteArrayCodec.BINARY_PREFIX);
    byte[] array = new byte[16384];
    int len;
    InputStream stream = value;
    boolean noBackslashEscapes =
        (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0;

    // readNBytes fills the chunk (several reads if the stream returns short reads), so each
    // escaped write handles a full chunk
    if (maxLen == null) {
      while ((len = stream.readNBytes(array, 0, array.length)) > 0) {
        encoder.writeBytesEscaped(array, len, noBackslashEscapes);
      }
    } else {
      long remaining = maxLen;
      while (remaining > 0
          && (len = stream.readNBytes(array, 0, (int) Math.min(array.length, remaining))) > 0) {
        encoder.writeBytesEscaped(array, len, noBackslashEscapes);
        remaining -= len;
      }
    }
    encoder.writeByte('\'');
  }

  @Override
  public int getApproximateTextProtocolLength(InputStream value, Long length) {
    return -1;
  }

  @Override
  public void encodeBinary(
      final Writer encoder,
      final Context context,
      final InputStream value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    // length is not known: read everything (or up to maxLength) with the JDK's buffered readers
    byte[] blobBytes =
        maxLength == null
            ? value.readAllBytes()
            : value.readNBytes((int) Math.max(0, Math.min(maxLength, Integer.MAX_VALUE)));
    encoder.writeLength(blobBytes.length);
    encoder.writeBytes(blobBytes, 0, blobBytes.length);
  }

  @Override
  public void encodeLongData(Writer encoder, InputStream value, Long maxLength) throws IOException {
    byte[] array = new byte[16384];
    int len;
    if (maxLength == null) {
      while ((len = value.readNBytes(array, 0, array.length)) > 0) {
        encoder.writeBytes(array, 0, len);
      }
    } else {
      long remaining = maxLength;
      while (remaining > 0
          && (len = value.readNBytes(array, 0, (int) Math.min(array.length, remaining))) > 0) {
        encoder.writeBytes(array, 0, len);
        remaining -= len;
      }
    }
  }

  @Override
  public byte[] encodeData(InputStream value, Long maxLength) throws IOException {
    return maxLength == null
        ? value.readAllBytes()
        : value.readNBytes((int) Math.max(0, Math.min(maxLength, Integer.MAX_VALUE)));
  }

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }

  public boolean canEncodeLongData() {
    return true;
  }
}
