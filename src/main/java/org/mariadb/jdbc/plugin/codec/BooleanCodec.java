// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** Boolean codec */
public class BooleanCodec implements Codec<Boolean> {

  /** default instance */
  public static final BooleanCodec INSTANCE = new BooleanCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING,
          DataType.BIGINT,
          DataType.INTEGER,
          DataType.MEDIUMINT,
          DataType.SMALLINT,
          DataType.YEAR,
          DataType.TINYINT,
          DataType.DECIMAL,
          DataType.OLDDECIMAL,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.BIT,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Boolean.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Boolean.TYPE) || type.isAssignableFrom(Boolean.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Boolean;
  }

  public Boolean decodeText(
      final ReadableByteBuf buffer,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal)
      throws SQLDataException {
    return column.decodeBooleanText(buffer, length);
  }

  public Boolean decodeBinary(
      final ReadableByteBuf buffer,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal)
      throws SQLDataException {
    return column.decodeBooleanBinary(buffer, length);
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeAscii(((Boolean) value) ? "1" : "0");
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeByte(((Boolean) value) ? 1 : 0);
  }

  public int getBinaryEncodeType() {
    return DataType.TINYINT.get();
  }
}
