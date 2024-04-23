// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.util.constants.ServerStatus;

/** Byte array codec. */
public class ByteArrayCodec implements Codec<byte[]> {

  /** binary prefix */
  public static final byte[] BINARY_PREFIX = {'_', 'b', 'i', 'n', 'a', 'r', 'y', ' ', '\''};

  /** default instance */
  public static final ByteArrayCodec INSTANCE = new ByteArrayCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.BIT,
          DataType.GEOMETRY,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING);

  public String className() {
    return "byte[]";
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Byte.TYPE && type.isArray())
            || type.isAssignableFrom(byte[].class));
  }

  public boolean canEncode(Object value) {
    return value instanceof byte[];
  }

  @Override
  public byte[] decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    return getBytes(buf, length, column);
  }

  private byte[] getBytes(ReadableByteBuf buf, MutableInt length, ColumnDecoder column)
      throws SQLDataException {
    switch (column.getType()) {
      case BIT:
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case STRING:
      case VARSTRING:
      case VARCHAR:
      case GEOMETRY:
        byte[] arr = new byte[length.get()];
        buf.readBytes(arr);
        return arr;

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as byte[]", column.getType()));
    }
  }

  @Override
  public byte[] decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    return getBytes(buf, length, column);
  }

  @Override
  public void encodeText(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    int length = ((byte[]) value).length;

    encoder.writeBytes(BINARY_PREFIX);
    encoder.writeBytesEscaped(
        ((byte[]) value),
        maxLength == null ? length : Math.min(length, maxLength.intValue()),
        (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    int length = ((byte[]) value).length;
    if (maxLength != null) length = Math.min(length, maxLength.intValue());
    encoder.writeLength(length);
    encoder.writeBytes(((byte[]) value), 0, length);
  }

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
