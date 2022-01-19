// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.Column;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Writer;
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
    return byte[].class.getName();
  }

  public boolean canDecode(Column column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Byte.TYPE && type.isArray())
            || type.isAssignableFrom(byte[].class));
  }

  public boolean canEncode(Object value) {
    return value instanceof byte[];
  }

  @Override
  public byte[] decodeText(ReadableByteBuf buf, int length, Column column, Calendar cal)
      throws SQLDataException {
    return getBytes(buf, length, column);
  }

  private byte[] getBytes(ReadableByteBuf buf, int length, Column column) throws SQLDataException {
    switch (column.getType()) {
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case STRING:
      case VARSTRING:
      case VARCHAR:
      case GEOMETRY:
        byte[] arr = new byte[length];
        buf.readBytes(arr);
        return arr;

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as byte[]", column.getType()));
    }
  }

  @Override
  public byte[] decodeBinary(ReadableByteBuf buf, int length, Column column, Calendar cal)
      throws SQLDataException {
    return getBytes(buf, length, column);
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object value, Calendar cal, Long maxLength)
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
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
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
