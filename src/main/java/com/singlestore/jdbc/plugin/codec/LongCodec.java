// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

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

public class LongCodec implements Codec<Long> {

  /** default instance */
  public static final LongCodec INSTANCE = new LongCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.OLDDECIMAL,
          DataType.VARCHAR,
          DataType.DECIMAL,
          DataType.ENUM,
          DataType.CHAR,
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INT,
          DataType.BIGINT,
          DataType.BIT,
          DataType.YEAR,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Long.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Integer.TYPE) || type.isAssignableFrom(Long.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Long;
  }

  @Override
  public Long decodeText(
      final ReadableByteBuf buffer,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal)
      throws SQLDataException {
    return column.decodeLongText(buffer, length);
  }

  @Override
  public Long decodeBinary(
      final ReadableByteBuf buffer,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal)
      throws SQLDataException {
    return column.decodeLongBinary(buffer, length);
  }

  @Override
  public void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    encoder.writeAscii(value.toString());
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeLong((Long) value);
  }

  public int getBinaryEncodeType() {
    return DataType.BIGINT.get();
  }
}
