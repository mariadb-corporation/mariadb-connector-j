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
import org.mariadb.jdbc.client.DataType;
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

  @Override
  public int setPosition(
      final int newIndex,
      final MutableInt fieldIndex,
      final int maxIndex,
      final ReadableByteBuf rowBuf,
      final byte[] nullBitmap,
      final ColumnDecoder[] metadataList)
      throws SQLException {
    int fi = fieldIndex.get();
    if (fi < 0 || fi >= newIndex) {
      // first access on this row, or going backward: restart after the 0x00 header + null-bitmap
      fi = 0;
      rowBuf.pos(1);
      for (int i = 0; i < nullBitmap.length; i++) nullBitmap[i] = rowBuf.readByte();
    } else {
      fi++;
    }
    if (fi < newIndex) fi = skipTo(fi, newIndex, rowBuf, nullBitmap, metadataList);
    fieldIndex.set(fi);

    int idx = fi + 2;
    if ((nullBitmap[idx >> 3] & (1 << (idx & 7))) != 0) return NULL_LENGTH;

    int fieldLength = fixedLength(metadataList[fi].getType());
    if (fieldLength == 0) fieldLength = readVariableLength(rowBuf);
    if (fieldLength > rowBuf.readableBytes()) throw invalidLength(fieldLength, rowBuf);
    return fieldLength;
  }

  private static int skipTo(
      int fi,
      final int newIndex,
      final ReadableByteBuf rowBuf,
      final byte[] nullBitmap,
      final ColumnDecoder[] metadataList) {
    while (fi < newIndex) {
      int idx = fi + 2;
      if ((nullBitmap[idx >> 3] & (1 << (idx & 7))) == 0) {
        int len = fixedLength(metadataList[fi].getType());
        if (len == 0) {
          rowBuf.skipLengthEncoded();
        } else {
          rowBuf.skip(len);
        }
      }
      fi++;
    }
    return fi;
  }

  /** binary-protocol fixed field length for the type, 0 when the field is length-encoded */
  private static int fixedLength(final DataType type) {
    switch (type) {
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
        return 0;
    }
  }

  private static int readVariableLength(final ReadableByteBuf rowBuf) throws SQLException {
    byte len = rowBuf.readByte();
    switch (len) {
      case (byte) 252:
        return rowBuf.readUnsignedShort();
      case (byte) 253:
        return rowBuf.readUnsignedMedium();
      case (byte) 254:
        long longLength = rowBuf.readLong();
        if (longLength < 0 || longLength > Integer.MAX_VALUE) {
          throw new SQLException("Invalid length-encoded field length " + longLength);
        }
        return (int) longLength;
      default:
        return len & 0xff;
    }
  }

  private static SQLException invalidLength(final int fieldLength, final ReadableByteBuf rowBuf) {
    return new SQLException(
        "Invalid length-encoded field length "
            + fieldLength
            + ": exceeds the "
            + rowBuf.readableBytes()
            + " bytes remaining in row packet");
  }
}
