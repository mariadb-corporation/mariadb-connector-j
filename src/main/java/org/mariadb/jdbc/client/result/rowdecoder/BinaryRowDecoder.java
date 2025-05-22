// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.result.rowdecoder;

import static org.mariadb.jdbc.client.result.Result.NULL_LENGTH;

import java.io.IOException;
import java.sql.*;
import java.util.Calendar;
import org.mariadb.jdbc.MariaDbResultSet;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** BINARY rows decoder */
public class BinaryRowDecoder implements RowDecoder {

  /**
   * Binary decode data according to data type.
   *
   * @param codec current codec
   * @param cal calendar
   * @param rowBuf row buffer
   * @param fieldLength field length
   * @param metadataList metadatas
   * @param fieldIndex field index
   * @param context connection context
   * @return default object according to metadata
   * @param <T> Codec default return type
   * @throws SQLException if any decoding error occurs
   */
  public <T> T decode(
      final Codec<T> codec,
      final Calendar cal,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final Context context)
      throws SQLException, IOException {
    return codec.decodeBinary(rowBuf, fieldLength, metadataList[fieldIndex.get()], cal, context);
  }

  @Override
  public Object defaultDecode(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Context context)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].getDefaultBinary(rowBuf, fieldLength, context);
  }

  public String decodeString(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Context context)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeStringBinary(rowBuf, fieldLength, null, context);
  }

  public byte decodeByte(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeByteBinary(rowBuf, fieldLength);
  }

  public boolean decodeBoolean(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeBooleanBinary(rowBuf, fieldLength);
  }

  public Date decodeDate(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Calendar cal,
      final Context context)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeDateBinary(rowBuf, fieldLength, cal, context);
  }

  public Time decodeTime(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Calendar cal,
      final Context context)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeTimeBinary(rowBuf, fieldLength, cal, context);
  }

  public Timestamp decodeTimestamp(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Calendar cal,
      final Context context)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeTimestampBinary(rowBuf, fieldLength, cal, context);
  }

  public short decodeShort(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeShortBinary(rowBuf, fieldLength);
  }

  public int decodeInt(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeIntBinary(rowBuf, fieldLength);
  }

  public long decodeLong(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeLongBinary(rowBuf, fieldLength);
  }

  public float decodeFloat(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeFloatBinary(rowBuf, fieldLength);
  }

  public double decodeDouble(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeDoubleBinary(rowBuf, fieldLength);
  }

  public boolean wasNull(
      final byte[] nullBitmap, final MutableInt fieldIndex, final MutableInt fieldLength) {
    return (nullBitmap[(fieldIndex.get() + 2) / 8] & (1 << ((fieldIndex.get() + 2) % 8))) > 0
        || fieldLength.get() == NULL_LENGTH;
  }

  /**
   * Set length and pos indicator to asked index.
   *
   * @param newIndex index (0 is first).
   */
  @Override
  public int setPosition(
      final int newIndex,
      final MutableInt fieldIndex,
      final int maxIndex,
      final ReadableByteBuf rowBuf,
      final byte[] nullBitmap,
      final ColumnDecoder[] metadataList,
      final int resultSetType)
      throws IOException, SQLException {

    if (fieldIndex.get() >= newIndex) {
      if (resultSetType == MariaDbResultSet.TYPE_SEQUENTIAL_ACCESS_ONLY) {
        throw new SQLException("Column decoder forward only when using SEQUENTIAL_ACCESS_ONLY");
      }
      fieldIndex.set(0);
      rowBuf.pos(1);
      rowBuf.readBytes(nullBitmap);
    } else {
      fieldIndex.incrementAndGet();
      if (fieldIndex.get() == 0) {
        // skip header + null-bitmap
        rowBuf.pos(1);
        rowBuf.readBytes(nullBitmap);
      }
    }

    while (fieldIndex.get() < newIndex) {
      if ((nullBitmap[(fieldIndex.get() + 2) / 8] & (1 << ((fieldIndex.get() + 2) % 8))) == 0) {
        // skip bytes
        switch (metadataList[fieldIndex.get()].getType()) {
          case BIGINT:
          case DOUBLE:
            rowBuf.skip(8);
            break;

          case INTEGER:
          case MEDIUMINT:
          case FLOAT:
            rowBuf.skip(4);
            break;

          case SMALLINT:
          case YEAR:
            rowBuf.skip(2);
            break;

          case TINYINT:
            rowBuf.skip(1);
            break;

          default:
            rowBuf.skipLengthEncoded();
            break;
        }
      }
      fieldIndex.incrementAndGet();
    }

    if ((nullBitmap[(fieldIndex.get() + 2) / 8] & (1 << ((fieldIndex.get() + 2) % 8))) > 0) {
      return NULL_LENGTH;
    }

    // read asked field position and length
    switch (metadataList[fieldIndex.get()].getType()) {
      case BIGINT:
      case DOUBLE:
        return 8;

      case INTEGER:
      case MEDIUMINT:
      case FLOAT:
        return 4;

      case SMALLINT:
      case YEAR:
        return 2;

      case TINYINT:
        return 1;

      default:
        // field with variable length
        byte len = rowBuf.readByte();
        switch (len) {
          case (byte) 252:
            // length is encoded on 3 bytes (0xfc header + 2 bytes indicating length)
            return rowBuf.readUnsignedShort();

          case (byte) 253:
            // length is encoded on 4 bytes (0xfd header + 3 bytes indicating length)
            return rowBuf.readUnsignedMedium();

          case (byte) 254:
            // length is encoded on 9 bytes (0xfe header + 8 bytes indicating length)
            return (int) rowBuf.readLong();
          default:
            return len & 0xff;
        }
    }
  }
}
