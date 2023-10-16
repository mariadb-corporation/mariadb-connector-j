// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.client.result.rowdecoder;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** Row decoder interface */
public interface RowDecoder {
  /**
   * Indicate if last decoded value was null
   *
   * @param nullBitmap null bitmap
   * @param fieldIndex field index
   * @param fieldLength field length
   * @return true if last value was null
   */
  boolean wasNull(byte[] nullBitmap, MutableInt fieldIndex, MutableInt fieldLength);

  /**
   * Position the read index on buffer to data at indicated index.
   *
   * @param newIndex new data index
   * @param fieldIndex current field index
   * @param maxIndex maximum index
   * @param rowBuf row buffer
   * @param nullBitmap null bitmap
   * @param metadataList metadata list
   * @return new index to read data
   */
  int setPosition(
      int newIndex,
      MutableInt fieldIndex,
      int maxIndex,
      StandardReadableByteBuf rowBuf,
      byte[] nullBitmap,
      ColumnDecoder[] metadataList);

  /**
   * Decode data according to data type.
   *
   * @param codec current codec
   * @param calendar calendar
   * @param rowBuf row buffer
   * @param fieldLength field length
   * @param metadataList metadatas
   * @param fieldIndex field index
   * @return default object according to metadata
   * @param <T> Codec default return type
   * @throws SQLException if any decoding error occurs
   */
  <T> T decode(
      Codec<T> codec,
      Calendar calendar,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength,
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex)
      throws SQLException;

  /**
   * Decode data according to data type.
   *
   * @param conf configuration
   * @param metadataList metadata list
   * @param fieldIndex field index
   * @param rowBuf row buffer
   * @param fieldLength field length
   * @return data
   * @throws SQLException if any decoding error occurs
   */
  Object defaultDecode(
      Configuration conf,
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException;

  /**
   * Decode data according to byte.
   *
   * @param metadataList metadata list
   * @param fieldIndex field index
   * @param rowBuf row buffer
   * @param fieldLength field length
   * @return data
   * @throws SQLException if data type cannot be decoded to byte value
   */
  byte decodeByte(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException;

  /**
   * Decode data according to boolean.
   *
   * @param metadataList metadata list
   * @param fieldIndex field index
   * @param rowBuf row buffer
   * @param fieldLength field length
   * @return data
   * @throws SQLException if data type cannot be decoded to boolean value
   */
  boolean decodeBoolean(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException;

  /**
   * Decode data according to Date.
   *
   * @param metadataList metadata list
   * @param fieldIndex field index
   * @param rowBuf row buffer
   * @param fieldLength field length
   * @param cal calendar
   * @return data
   * @throws SQLException if data type cannot be decoded to Date value
   */
  Date decodeDate(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength,
      Calendar cal)
      throws SQLException;

  /**
   * Decode data according to Time.
   *
   * @param metadataList metadata list
   * @param fieldIndex field index
   * @param rowBuf row buffer
   * @param fieldLength field length
   * @param cal calendar
   * @return data
   * @throws SQLException if data type cannot be decoded to Time value
   */
  Time decodeTime(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength,
      Calendar cal)
      throws SQLException;

  /**
   * Decode data according to Timestamp.
   *
   * @param metadataList metadata list
   * @param fieldIndex field index
   * @param rowBuf row buffer
   * @param fieldLength field length
   * @param cal calendar
   * @return data
   * @throws SQLException if data type cannot be decoded to Timestamp value
   */
  Timestamp decodeTimestamp(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength,
      Calendar cal)
      throws SQLException;

  /**
   * Decode data according to short.
   *
   * @param metadataList metadata list
   * @param fieldIndex field index
   * @param rowBuf row buffer
   * @param fieldLength field length
   * @return data
   * @throws SQLException if data type cannot be decoded to short value
   */
  short decodeShort(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException;

  /**
   * Decode data according to int.
   *
   * @param metadataList metadata list
   * @param fieldIndex field index
   * @param rowBuf row buffer
   * @param fieldLength field length
   * @return data
   * @throws SQLException if data type cannot be decoded to int value
   */
  int decodeInt(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException;

  /**
   * Decode data according to String.
   *
   * @param metadataList metadata list
   * @param fieldIndex field index
   * @param rowBuf row buffer
   * @param fieldLength field length
   * @return data
   * @throws SQLException if data type cannot be decoded to String value
   */
  String decodeString(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException;

  /**
   * Decode data according to long.
   *
   * @param metadataList metadata list
   * @param fieldIndex field index
   * @param rowBuf row buffer
   * @param fieldLength field length
   * @return data
   * @throws SQLException if data type cannot be decoded to long value
   */
  long decodeLong(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException;

  /**
   * Decode data according to float.
   *
   * @param metadataList metadata list
   * @param fieldIndex field index
   * @param rowBuf row buffer
   * @param fieldLength field length
   * @return data
   * @throws SQLException if data type cannot be decoded to float value
   */
  float decodeFloat(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException;

  /**
   * Decode data according to double.
   *
   * @param metadataList metadata list
   * @param fieldIndex field index
   * @param rowBuf row buffer
   * @param fieldLength field length
   * @return data
   * @throws SQLException if data type cannot be decoded to double value
   */
  double decodeDouble(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException;
}
