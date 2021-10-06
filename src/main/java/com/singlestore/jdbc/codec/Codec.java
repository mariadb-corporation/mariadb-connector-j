// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.codec;

import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.context.Context;
import com.singlestore.jdbc.client.socket.PacketWriter;
import com.singlestore.jdbc.message.server.ColumnDefinitionPacket;
import java.io.IOException;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Calendar;

public interface Codec<T> {

  String className();

  boolean canDecode(ColumnDefinitionPacket column, Class<?> type);

  boolean canEncode(Object value);

  T decodeText(
      final ReadableByteBuf buffer,
      final int length,
      final ColumnDefinitionPacket column,
      final Calendar cal)
      throws SQLDataException;

  T decodeBinary(
      final ReadableByteBuf buffer,
      final int length,
      final ColumnDefinitionPacket column,
      final Calendar cal)
      throws SQLDataException;

  void encodeText(PacketWriter encoder, Context context, Object value, Calendar cal, Long length)
      throws IOException, SQLException;

  void encodeBinary(PacketWriter encoder, Object value, Calendar cal, Long length)
      throws IOException, SQLException;

  default boolean canEncodeLongData() {
    return false;
  }

  default void encodeLongData(PacketWriter encoder, T value, Long length)
      throws IOException, SQLException {
    throw new SQLException("Data is not supposed to be send in COM_STMT_LONG_DATA");
  }

  default byte[] encodeData(T value, Long length) throws IOException, SQLException {
    throw new SQLException("Data is not supposed to be send in COM_STMT_LONG_DATA");
  }

  int getBinaryEncodeType();
}
