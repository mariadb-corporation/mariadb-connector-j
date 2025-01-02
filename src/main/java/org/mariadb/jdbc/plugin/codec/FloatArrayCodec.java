// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.util.constants.ServerStatus;

/** Float codec */
public class FloatArrayCodec implements Codec<float[]> {

  /** default instance */
  public static final FloatArrayCodec INSTANCE = new FloatArrayCodec();

  private static Class<?> floatArrayClass = Array.newInstance(float.class, 0).getClass();

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
    return value instanceof float[];
  }

  @Override
  public float[] decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {

    return toFloatArray(getBytes(buf, length, column));
  }

  @Override
  public float[] decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {

    return toFloatArray(getBytes(buf, length, column));
  }

  static final int BYTES_IN_FLOAT = Float.SIZE / Byte.SIZE;

  public static byte[] toByteArray(float[] floatArray) {
    ByteBuffer buffer = ByteBuffer.allocate(floatArray.length * BYTES_IN_FLOAT);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.asFloatBuffer().put(floatArray);
    return buffer.array();
  }

  public static float[] toFloatArray(byte[] byteArray) {
    float[] result = new float[byteArray.length / BYTES_IN_FLOAT];
    ByteBuffer.wrap(byteArray)
        .order(ByteOrder.LITTLE_ENDIAN)
        .asFloatBuffer()
        .get(result, 0, result.length);
    return result;
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
            String.format("Data type %s cannot be decoded as float[]", column.getType()));
    }
  }

  @Override
  public void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    byte[] encoded = toByteArray((float[]) value);
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
    byte[] arr = toByteArray((float[]) value);
    encoder.writeLength(arr.length);
    encoder.writeBytes(arr);
  }

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
