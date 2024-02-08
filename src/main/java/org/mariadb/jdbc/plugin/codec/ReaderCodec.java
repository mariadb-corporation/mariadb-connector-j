// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.util.constants.ServerStatus;

/** Reader codec */
public class ReaderCodec implements Codec<Reader> {

  /** default instance */
  public static final ReaderCodec INSTANCE = new ReaderCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.STRING,
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Reader.class);
  }

  public String className() {
    return Reader.class.getName();
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Reader decodeText(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    switch (column.getType()) {
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length.get());
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Reader", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if it has a collation (this is TEXT column)

      case STRING:
      case VARCHAR:
      case VARSTRING:
        return new StringReader(buf.readString(length.get()));

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Reader", column.getType()));
    }
  }

  @Override
  public Reader decodeBinary(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    return decodeText(buf, length, column, cal);
  }

  public boolean canEncode(Object value) {
    return value instanceof Reader;
  }

  @Override
  public void encodeText(Writer encoder, Context context, Object val, Calendar cal, Long maxLen)
      throws IOException {
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
  public void encodeBinary(Writer encoder, Object val, Calendar cal, Long maxLength)
      throws IOException {
    // prefer use of encodeLongData, because length is unknown
    byte[] clobBytes = new byte[4096];
    int pos = 0;
    char[] buf = new char[4096];
    Reader reader = (Reader) val;
    int len;

    long maxLen = maxLength != null ? maxLength : Long.MAX_VALUE;
    while (maxLen > 0 && (len = reader.read(buf)) > 0) {
      byte[] data =
          new String(buf, 0, (int) Math.min(len, maxLen)).getBytes(StandardCharsets.UTF_8);
      if (clobBytes.length - pos < data.length) {
        byte[] newBlobBytes = new byte[clobBytes.length + 65536];
        System.arraycopy(clobBytes, 0, newBlobBytes, 0, pos);
        clobBytes = newBlobBytes;
      }
      System.arraycopy(data, 0, clobBytes, pos, data.length);
      pos += data.length;
      maxLen -= len;
    }
    encoder.writeLength(pos);
    encoder.writeBytes(clobBytes, 0, pos);
  }

  @Override
  public void encodeLongData(Writer encoder, Reader reader, Long maxLength) throws IOException {
    char[] buf = new char[4096];
    int len;
    long maxLen = maxLength != null ? maxLength : Long.MAX_VALUE;
    while (maxLen > 0 && (len = reader.read(buf)) >= 0) {
      byte[] data =
          new String(buf, 0, (int) Math.min(len, maxLen)).getBytes(StandardCharsets.UTF_8);
      encoder.writeBytes(data, 0, data.length);
      maxLen -= len;
    }
  }

  @Override
  public byte[] encodeData(Reader reader, Long maxLength) throws IOException {
    ByteArrayOutputStream bb = new ByteArrayOutputStream();
    char[] buf = new char[4096];
    int len;
    long maxLen = maxLength != null ? maxLength : Long.MAX_VALUE;
    while (maxLen > 0 && (len = reader.read(buf)) >= 0) {
      byte[] data =
          new String(buf, 0, (int) Math.min(len, maxLen)).getBytes(StandardCharsets.UTF_8);
      bb.write(data, 0, data.length);
      maxLen -= len;
    }
    return bb.toByteArray();
  }

  public int getBinaryEncodeType() {
    return DataType.VARSTRING.get();
  }

  public boolean canEncodeLongData() {
    return true;
  }
}
