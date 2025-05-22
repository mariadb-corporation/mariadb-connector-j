// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.result.rowdecoder;

import static org.mariadb.jdbc.client.result.Result.NULL_LENGTH;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import org.mariadb.jdbc.MariaDbResultSet;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** TEXT rows decoder */
public class TextRowDecoder implements RowDecoder {

  @Override
  public <T> T decode(
      final Codec<T> codec,
      final Calendar cal,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final Context context)
      throws SQLException, IOException {
    return codec.decodeText(rowBuf, fieldLength, metadataList[fieldIndex.get()], cal, context);
  }

  @Override
  public Object defaultDecode(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Context context)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].getDefaultText(rowBuf, fieldLength, context);
  }

  @Override
  public String decodeString(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Context context)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeStringText(rowBuf, fieldLength, null, context);
  }

  public byte decodeByte(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeByteText(rowBuf, fieldLength);
  }

  public boolean decodeBoolean(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeBooleanText(rowBuf, fieldLength);
  }

  public Date decodeDate(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Calendar cal,
      final Context context)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeDateText(rowBuf, fieldLength, cal, context);
  }

  public Time decodeTime(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Calendar cal,
      final Context context)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeTimeText(rowBuf, fieldLength, cal, context);
  }

  public Timestamp decodeTimestamp(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength,
      final Calendar cal,
      final Context context)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeTimestampText(rowBuf, fieldLength, cal, context);
  }

  public short decodeShort(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeShortText(rowBuf, fieldLength);
  }

  public int decodeInt(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeIntText(rowBuf, fieldLength);
  }

  public long decodeLong(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeLongText(rowBuf, fieldLength);
  }

  public float decodeFloat(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeFloatText(rowBuf, fieldLength);
  }

  public double decodeDouble(
      final ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      final ReadableByteBuf rowBuf,
      final MutableInt fieldLength)
      throws SQLException, IOException {
    return metadataList[fieldIndex.get()].decodeDoubleText(rowBuf, fieldLength);
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
      rowBuf.pos(0);
    } else {
      fieldIndex.incrementAndGet();
    }

    while (fieldIndex.get() < newIndex) {
      rowBuf.skipLengthEncoded();
      fieldIndex.incrementAndGet();
    }

    byte len = rowBuf.readByte();
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
