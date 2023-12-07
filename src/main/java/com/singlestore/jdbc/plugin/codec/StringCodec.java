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
import com.singlestore.jdbc.util.constants.ServerStatus;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;

public class StringCodec implements Codec<String> {

  /** default instance */
  public static final StringCodec INSTANCE = new StringCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BIT,
          DataType.OLDDECIMAL,
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.INT,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.TIMESTAMP,
          DataType.BIGINT,
          DataType.MEDIUMINT,
          DataType.DATE,
          DataType.TIME,
          DataType.DATETIME,
          DataType.YEAR,
          DataType.NEWDATE,
          DataType.JSON,
          DataType.DECIMAL,
          DataType.ENUM,
          DataType.SET,
          DataType.VARCHAR,
          DataType.CHAR,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return String.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(String.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof String;
  }

  public String decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal)
      throws SQLDataException {
    return column.decodeStringText(buf, length, cal);
  }

  public String decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal)
      throws SQLDataException {
    return column.decodeStringBinary(buf, length, cal);
  }

  public void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    encoder.writeByte('\'');
    encoder.writeStringEscaped(
        maxLen == null ? value.toString() : value.toString().substring(0, maxLen.intValue()),
        (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
    encoder.writeByte('\'');
  }

  public void encodeBinary(Writer writer, Object value, Calendar cal, Long maxLength)
      throws IOException {
    byte[] b = value.toString().getBytes(StandardCharsets.UTF_8);
    int len = maxLength != null ? Math.min(maxLength.intValue(), b.length) : b.length;
    writer.writeLength(len);
    writer.writeBytes(b, 0, len);
  }

  public int getBinaryEncodeType() {
    return DataType.VARCHAR.get();
  }
}
