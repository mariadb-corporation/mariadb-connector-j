// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.plugin;

import java.io.IOException;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Calendar;
import org.mariadb.jdbc.client.Column;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Writer;

public interface Codec<T> {

  String className();

  boolean canDecode(Column column, Class<?> type);

  boolean canEncode(Object value);

  T decodeText(
      final ReadableByteBuf buffer, final int length, final Column column, final Calendar cal)
      throws SQLDataException;

  T decodeBinary(
      final ReadableByteBuf buffer, final int length, final Column column, final Calendar cal)
      throws SQLDataException;

  void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long length)
      throws IOException, SQLException;

  void encodeBinary(Writer encoder, Object value, Calendar cal, Long length)
      throws IOException, SQLException;

  default boolean canEncodeLongData() {
    return false;
  }

  default void encodeLongData(Writer encoder, T value, Long length)
      throws IOException, SQLException {
    throw new SQLException("Data is not supposed to be send in COM_STMT_LONG_DATA");
  }

  default byte[] encodeData(T value, Long length) throws IOException, SQLException {
    throw new SQLException("Data is not supposed to be send in COM_STMT_LONG_DATA");
  }

  int getBinaryEncodeType();
}
