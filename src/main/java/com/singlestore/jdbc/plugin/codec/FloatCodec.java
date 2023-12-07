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

public class FloatCodec implements Codec<Float> {

  /** default instance */
  public static final FloatCodec INSTANCE = new FloatCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INT,
          DataType.FLOAT,
          DataType.BIGINT,
          DataType.OLDDECIMAL,
          DataType.DECIMAL,
          DataType.YEAR,
          DataType.DOUBLE,
          DataType.VARCHAR,
          DataType.CHAR,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Float.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Float.TYPE) || type.isAssignableFrom(Float.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Float;
  }

  @Override
  public Float decodeText(
      final ReadableByteBuf buffer,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal)
      throws SQLDataException {
    return column.decodeFloatText(buffer, length);
  }

  @Override
  public Float decodeBinary(
      final ReadableByteBuf buffer,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal)
      throws SQLDataException {
    return column.decodeFloatBinary(buffer, length);
  }

  @Override
  public void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    encoder.writeAscii(value.toString());
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeFloat((Float) value);
  }

  public int getBinaryEncodeType() {
    return DataType.FLOAT.get();
  }
}
