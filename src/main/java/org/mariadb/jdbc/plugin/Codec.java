// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin;

import java.io.IOException;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Calendar;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;

/**
 * Codec interface, to describe how a certain type of data must be encoded / decoded
 *
 * @param <T> java type supported
 */
public interface Codec<T> {

  /**
   * Codec native type
   *
   * @return code native return type
   */
  String className();

  /**
   * If codec can decode this a server datatype to a java class type
   *
   * @param column server datatype
   * @param type java return class
   * @return true if codec can decode it
   */
  boolean canDecode(ColumnDecoder column, Class<?> type);

  /**
   * Can Codec encode the java object type
   *
   * @param value java object type
   * @return true if codec can encode java type
   */
  boolean canEncode(Object value);

  /**
   * Decode from a mysql packet text encoded a value to codec java type
   *
   * @param buffer mysql packet buffer
   * @param fieldLength encoded value length
   * @param column server column metadata
   * @param cal calendar
   * @param context connection context
   * @return decoded value
   * @throws SQLDataException if unexpected error occurs during decoding
   */
  T decodeText(
      final ReadableByteBuf buffer,
      final MutableInt fieldLength,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException, IOException;

  /**
   * Decode from a mysql packet binary encoded a value to codec java type
   *
   * @param buffer mysql packet buffer
   * @param fieldLength encoded value length
   * @param column server column metadata
   * @param cal calendar
   * @param context connection context
   * @return decoded value
   * @throws SQLDataException if unexpected error occurs during decoding
   */
  T decodeBinary(
      final ReadableByteBuf buffer,
      final MutableInt fieldLength,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException, IOException;

  /**
   * Text encode value to writer
   *
   * @param encoder writer
   * @param context connection context
   * @param value value to encode
   * @param cal calendar
   * @param length maximum value length
   * @throws IOException if any socket error occurs
   * @throws SQLException if encoding error occurs
   */
  void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long length)
      throws IOException, SQLException;

  /**
   * Binary encode value to writer
   *
   * @param encoder writer
   * @param context connection context
   * @param value value to encode
   * @param cal calendar
   * @param length maximum value length
   * @throws IOException if any socket error occurs
   * @throws SQLException if encoding error occurs
   */
  void encodeBinary(Writer encoder, Context context, Object value, Calendar cal, Long length)
      throws IOException, SQLException;

  /**
   * Indicate if can encode long data
   *
   * @return true if possible
   */
  default boolean canEncodeLongData() {
    return false;
  }

  /**
   * binary encoding value to a long data packet
   *
   * @param encoder writer
   * @param value value to encode
   * @param length maximum length value
   * @throws IOException if any socket error occurs
   * @throws SQLException if encoding error occurs
   */
  default void encodeLongData(Writer encoder, T value, Long length)
      throws IOException, SQLException {
    throw new SQLException("Data is not supposed to be send in COM_STMT_LONG_DATA");
  }

  /**
   * binary encoding value to a byte[]
   *
   * @param value value to encode
   * @param length maximum length value
   * @return encoded value
   * @throws IOException if any socket error occurs
   * @throws SQLException if encoding error occurs
   */
  default byte[] encodeData(T value, Long length) throws IOException, SQLException {
    throw new SQLException("Data is not supposed to be send in COM_STMT_LONG_DATA");
  }

  /**
   * Return server encoding data type
   *
   * @return server encoding data type
   */
  int getBinaryEncodeType();
}
