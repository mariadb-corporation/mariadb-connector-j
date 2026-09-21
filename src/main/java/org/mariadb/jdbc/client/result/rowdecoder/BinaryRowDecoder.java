// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.client.result.rowdecoder;

import static org.mariadb.jdbc.client.result.Result.NULL_LENGTH;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
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
      final int fieldIndex,
      final Context context)
      throws SQLException {
    return codec.decodeBinary(rowBuf, fieldLength, metadataList[fieldIndex], cal, context);
  }

  @Override
  public Object defaultDecode(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Context context)
      throws SQLException {
    return metadataList[fieldIndex].getDefaultBinary(rowBuf, fieldLength, context);
  }

  public String decodeString(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Context context)
      throws SQLException {
    return metadataList[fieldIndex].decodeStringBinary(rowBuf, fieldLength, null, context);
  }

  public byte decodeByte(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex].decodeByteBinary(rowBuf, fieldLength);
  }

  public boolean decodeBoolean(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex].decodeBooleanBinary(rowBuf, fieldLength);
  }

  public Date decodeDate(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Calendar cal,
      final Context context)
      throws SQLException {
    return metadataList[fieldIndex].decodeDateBinary(rowBuf, fieldLength, cal, context);
  }

  public Time decodeTime(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Calendar cal,
      final Context context)
      throws SQLException {
    return metadataList[fieldIndex].decodeTimeBinary(rowBuf, fieldLength, cal, context);
  }

  public Timestamp decodeTimestamp(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Calendar cal,
      final Context context)
      throws SQLException {
    return metadataList[fieldIndex].decodeTimestampBinary(rowBuf, fieldLength, cal, context);
  }

  public short decodeShort(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex].decodeShortBinary(rowBuf, fieldLength);
  }

  public int decodeInt(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex].decodeIntBinary(rowBuf, fieldLength);
  }

  public long decodeLong(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex].decodeLongBinary(rowBuf, fieldLength);
  }

  public float decodeFloat(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex].decodeFloatBinary(rowBuf, fieldLength);
  }

  public double decodeDouble(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex].decodeDoubleBinary(rowBuf, fieldLength);
  }

  public boolean wasNull(
      final byte[] nullBitmap, final MutableInt fieldIndex, final MutableInt fieldLength) {
    int idx = fieldIndex.get() + 2;
    return (nullBitmap[idx / 8] & (1 << (idx % 8))) > 0 || fieldLength.get() == NULL_LENGTH;
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
      final ColumnDecoder[] metadataList)
      throws SQLException {

    if (fieldIndex.get() >= newIndex) {
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
      int fi = fieldIndex.get();
      int idx = fi + 2;
      if ((nullBitmap[idx / 8] & (1 << (idx % 8))) == 0) {
        // skip bytes
        switch (metadataList[fi].getType()) {
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

    int fi = fieldIndex.get();
    int idx = fi + 2;
    if ((nullBitmap[idx / 8] & (1 << (idx % 8))) > 0) {
      return NULL_LENGTH;
    }

    // read asked field position and length
    int fieldLength;
    switch (metadataList[fi].getType()) {
      case BIGINT:
      case DOUBLE:
        fieldLength = 8;
        break;

      case INTEGER:
      case MEDIUMINT:
      case FLOAT:
        fieldLength = 4;
        break;

      case SMALLINT:
      case YEAR:
        fieldLength = 2;
        break;

      case TINYINT:
        fieldLength = 1;
        break;

      default:
        // field with variable length
        byte len = rowBuf.readByte();
        switch (len) {
          case (byte) 252:
            // length is encoded on 3 bytes (0xfc header + 2 bytes indicating length)
            fieldLength = rowBuf.readUnsignedShort();
            break;

          case (byte) 253:
            // length is encoded on 4 bytes (0xfd header + 3 bytes indicating length)
            fieldLength = rowBuf.readUnsignedMedium();
            break;

          case (byte) 254:
            // length is encoded on 9 bytes (0xfe header + 8 bytes indicating length)
            long longLength = rowBuf.readLong();
            if (longLength < 0 || longLength > Integer.MAX_VALUE) {
              throw new SQLException("Invalid length-encoded field length " + longLength);
            }
            fieldLength = (int) longLength;
            break;
          default:
            fieldLength = len & 0xff;
        }
    }
    // the length is server-declared and column decoders allocate from it: it must fit in the row
    if (fieldLength > rowBuf.readableBytes()) {
      throw new SQLException(
          "Invalid length-encoded field length "
              + fieldLength
              + ": exceeds the "
              + rowBuf.readableBytes()
              + " bytes remaining in row packet");
    }
    return fieldLength;
  }
}
