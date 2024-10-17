// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.util.constants.ServerStatus;

/** Float codec */
public class FloatObjectArrayCodec implements Codec<Float[]> {

  /** default instance */
  public static final FloatObjectArrayCodec INSTANCE = new FloatObjectArrayCodec();

  private static Class floatArrayClass = Array.newInstance(Float.class, 0).getClass();
  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING);

  public String className() {
    return float[].class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((!type.isPrimitive() && type == floatArrayClass && type.isArray()));
  }

  public boolean canEncode(Object value) {
    return value instanceof Float[];
  }

  @Override
  public Float[] decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {

    return toFloatArray(getBytes(buf, length, column));
  }

  @Override
  public Float[] decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {

    return toFloatArray(getBytes(buf, length, column));
  }

  static final int BYTES_IN_FLOAT = Float.SIZE / Byte.SIZE;

  public static byte[] toByteArray(Float[] floatArray) {
    byte[] buf = new byte[floatArray.length * BYTES_IN_FLOAT];
    int pos = 0;
    for (Float f : floatArray) {
      int value = Float.floatToIntBits(f);
      buf[pos] = (byte) value;
      buf[pos + 1] = (byte) (value >> 8);
      buf[pos + 2] = (byte) (value >> 16);
      buf[pos + 3] = (byte) (value >> 24);
      pos += 4;
    }
    return buf;
  }

  public static Float[] toFloatArray(byte[] byteArray) {
    int len = (int) Math.ceil(byteArray.length / 4.0);
    Float[] res = new Float[len];
    int pos = 0;
    int value;
    while (pos < len) {
      if (pos + 1 <= len) {
        value =
            ((byteArray[pos * 4] & 0xff)
                + ((byteArray[pos * 4 + 1] & 0xff) << 8)
                + ((byteArray[pos * 4 + 2] & 0xff) << 16)
                + ((byteArray[pos * 4 + 3] & 0xff) << 24));
      } else {
        value = (byteArray[pos * 4] & 0xff);
        if (pos + 1 < byteArray.length) value += ((byteArray[pos * 4 + 1] & 0xff) << 8);
        if (pos + 2 < byteArray.length) value += ((byteArray[pos * 4 + 2] & 0xff) << 16);
      }
      res[pos++] = Float.intBitsToFloat(value);
    }
    return res;
  }

  private byte[] getBytes(ReadableByteBuf buf, MutableInt length, ColumnDecoder column)
      throws SQLDataException {
    switch (column.getType()) {
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
  public void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    byte[] encoded = toByteArray((Float[]) value);
    encoder.writeBytes(ByteArrayCodec.BINARY_PREFIX);
    encoder.writeBytesEscaped(
        encoded,
        encoded.length,
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
    encoder.writeBytes(toByteArray((Float[]) value));
  }

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
