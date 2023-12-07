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
import java.io.IOException;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;

public class ByteCodec implements Codec<Byte> {

  /** default instance */
  public static final ByteCodec INSTANCE = new ByteCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INT,
          DataType.BIGINT,
          DataType.YEAR,
          DataType.BIT,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.OLDDECIMAL,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.DECIMAL,
          DataType.ENUM,
          DataType.CHAR,
          DataType.VARCHAR);

  /**
   * Parse Bits value to long value
   *
   * @param buf packet buffer
   * @param length encoded length
   * @return long value
   */
  public static long parseBit(ReadableByteBuf buf, MutableInt length) {
    if (length.get() == 1) {
      return buf.readUnsignedByte();
    }
    long val = 0;
    int idx = 0;
    do {
      val += ((long) buf.readUnsignedByte()) << (8 * length.get());
      idx++;
    } while (idx < length.get());
    return val;
  }

  public String className() {
    return Byte.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Byte.TYPE) || type.isAssignableFrom(Byte.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Byte;
  }

  @Override
  public Byte decodeText(
      final ReadableByteBuf buffer,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal)
      throws SQLDataException {
    return column.decodeByteText(buffer, length);
  }

  @Override
  public Byte decodeBinary(
      final ReadableByteBuf buffer,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal)
      throws SQLDataException {
    return column.decodeByteBinary(buffer, length);
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeAscii(Integer.toString((Byte) value));
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeByte((byte) value);
  }

  @Override
  public int getBinaryEncodeType() {
    return DataType.TINYINT.get();
  }
}
