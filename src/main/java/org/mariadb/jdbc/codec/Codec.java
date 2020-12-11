/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Calendar;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

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

  void encodeText(PacketWriter encoder, Context context, T value, Calendar cal, Long length)
      throws IOException, SQLException;

  void encodeBinary(PacketWriter encoder, Context context, T value, Calendar cal)
      throws IOException, SQLException;

  default boolean canEncodeLongData() {
    return false;
  }

  default void encodeLongData(PacketWriter encoder, Context context, T value, Long length)
      throws IOException, SQLException {
    throw new SQLException("Data is not supposed to be send in COM_STMT_LONG_DATA");
  }

  default byte[] encodeLongDataReturning(
      PacketWriter encoder, Context context, T value, Long length)
      throws IOException, SQLException {
    throw new SQLException("Data is not supposed to be send in COM_STMT_LONG_DATA");
  }

  DataType getBinaryEncodeType();
}
