// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.result.rowdecoder;

import static org.mariadb.jdbc.client.result.Result.NULL_LENGTH;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** TEXT rows decoder */
public class TextRowDecoder implements RowDecoder {

  @Override
  public <T> T decode(
      final Codec<T> codec,
      final Calendar cal,
      final StandardReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final Context context)
      throws SQLException {
    return codec.decodeText(rowBuf, fieldLength, metadataList[fieldIndex], cal, context);
  }

  @Override
  public Object defaultDecode(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final StandardReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Context context)
      throws SQLException {
    return metadataList[fieldIndex].getDefaultText(rowBuf, fieldLength, context);
  }

  @Override
  public String decodeString(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final StandardReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Context context)
      throws SQLException {
    return metadataList[fieldIndex].decodeStringText(rowBuf, fieldLength, null, context);
  }

  public byte decodeByte(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final StandardReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex].decodeByteText(rowBuf, fieldLength);
  }

  public boolean decodeBoolean(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final StandardReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex].decodeBooleanText(rowBuf, fieldLength);
  }

  public Date decodeDate(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final StandardReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Calendar cal,
      final Context context)
      throws SQLException {
    return metadataList[fieldIndex].decodeDateText(rowBuf, fieldLength, cal, context);
  }

  public Time decodeTime(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final StandardReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Calendar cal,
      final Context context)
      throws SQLException {
    return metadataList[fieldIndex].decodeTimeText(rowBuf, fieldLength, cal, context);
  }

  public Timestamp decodeTimestamp(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final StandardReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Calendar cal,
      final Context context)
      throws SQLException {
    return metadataList[fieldIndex].decodeTimestampText(rowBuf, fieldLength, cal, context);
  }

  public short decodeShort(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final StandardReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex].decodeShortText(rowBuf, fieldLength);
  }

  public int decodeInt(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final StandardReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex].decodeIntText(rowBuf, fieldLength);
  }

  public long decodeLong(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final StandardReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex].decodeLongText(rowBuf, fieldLength);
  }

  public float decodeFloat(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final StandardReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex].decodeFloatText(rowBuf, fieldLength);
  }

  public double decodeDouble(
      final ColumnDecoder[] metadataList,
      final int fieldIndex,
      final StandardReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex].decodeDoubleText(rowBuf, fieldLength);
  }

  public boolean wasNull(
      final byte[] nullBitmap, final MutableInt fieldIndex, final MutableInt fieldLength) {
    return fieldLength.get() == NULL_LENGTH;
  }

  /**
   * Set length and pos indicator to asked index.
   *
   * @param newIndex index (1 is first).
   */
  @Override
  public int setPosition(
      final int newIndex,
      final MutableInt fieldIndex,
      final int maxIndex,
      final StandardReadableByteBuf rowBuf,
      final byte[] nullBitmap,
      final ColumnDecoder[] metadataList) {
    if (fieldIndex.get() >= newIndex) {
      fieldIndex.set(0);
      rowBuf.pos(0);
    } else {
      fieldIndex.incrementAndGet();
    }

    while (fieldIndex.get() < newIndex) {
      rowBuf.skipLengthEncoded();
      fieldIndex.incrementAndGet();
    }

    byte len = rowBuf.buf[rowBuf.pos++];
    switch (len) {
      case (byte) 251:
        return NULL_LENGTH;
      case (byte) 252:
        return rowBuf.readUnsignedShort();
      case (byte) 253:
        return rowBuf.readUnsignedMedium();
      case (byte) 254:
        int fieldLength = (int) rowBuf.readUnsignedInt();
        rowBuf.skip(4);
        return fieldLength;
      default:
        return len & 0xff;
    }
  }
}
