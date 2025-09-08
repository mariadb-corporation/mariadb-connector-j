// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.util.BitSet;
import java.util.Calendar;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** BitSet Codec */
public class BitSetCodec implements Codec<BitSet> {

  /** default instance */
  public static final BitSetCodec INSTANCE = new BitSetCodec();

  /**
   * decode from mysql packet value to BitSet
   *
   * @param buf mysql packet buffer
   * @param length encoded length
   * @return BitSet value
   */
  public static BitSet parseBit(ReadableByteBuf buf, MutableInt length) {
    byte[] arr = new byte[length.get()];
    buf.readBytes(arr);
    revertOrder(arr);
    return BitSet.valueOf(arr);
  }

  /**
   * Revert byte array order
   *
   * @param array array to revert
   */
  public static void revertOrder(byte[] array) {
    int i = 0;
    int j = array.length - 1;
    byte tmp;
    while (j > i) {
      tmp = array[j];
      array[j] = array[i];
      array[i] = tmp;
      j--;
      i++;
    }
  }

  public String className() {
    return BitSet.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return column.getType() == DataType.BIT && type.isAssignableFrom(BitSet.class);
  }

  @Override
  public BitSet decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context) {
    return parseBit(buf, length);
  }

  @Override
  public BitSet decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context) {
    return parseBit(buf, length);
  }

  public boolean canEncode(Object value) {
    return value instanceof BitSet;
  }

  @Override
  public void encodeText(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long length)
      throws IOException {
    byte[] bytes = ((BitSet) value).toByteArray();
    revertOrder(bytes);

    StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE + 3);
    sb.append("b'");
    for (int i = 0; i < Byte.SIZE * bytes.length; i++)
      sb.append((bytes[i / Byte.SIZE] << i % Byte.SIZE & 0x80) == 0 ? '0' : '1');
    sb.append("'");
    encoder.writeAscii(sb.toString());
  }

  @Override
  public int getApproximateTextProtocolLength(Object value, Long length) {
    return ((BitSet) value).length() + 3;
  }

  @Override
  public void encodeBinary(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    byte[] bytes = ((BitSet) value).toByteArray();
    revertOrder(bytes);
    encoder.writeLength(bytes.length);
    encoder.writeBytes(bytes);
  }

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
