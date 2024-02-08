// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.client.result.rowdecoder;

import static org.mariadb.jdbc.client.result.Result.NULL_LENGTH;

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

/** TEXT rows decoder */
public class TextRowDecoder implements RowDecoder {

  @Override
  public <T> T decode(
      Codec<T> codec,
      Calendar cal,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength,
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex)
      throws SQLException {
    return codec.decodeText(rowBuf, fieldLength, metadataList[fieldIndex.get()], cal);
  }

  @Override
  public Object defaultDecode(
      Configuration conf,
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].getDefaultText(conf, rowBuf, fieldLength);
  }

  @Override
  public String decodeString(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeStringText(rowBuf, fieldLength, null);
  }

  public byte decodeByte(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeByteText(rowBuf, fieldLength);
  }

  public boolean decodeBoolean(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeBooleanText(rowBuf, fieldLength);
  }

  public Date decodeDate(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength,
      Calendar cal)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeDateText(rowBuf, fieldLength, cal);
  }

  public Time decodeTime(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength,
      Calendar cal)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeTimeText(rowBuf, fieldLength, cal);
  }

  public Timestamp decodeTimestamp(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength,
      Calendar cal)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeTimestampText(rowBuf, fieldLength, cal);
  }

  public short decodeShort(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeShortText(rowBuf, fieldLength);
  }

  public int decodeInt(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeIntText(rowBuf, fieldLength);
  }

  public long decodeLong(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeLongText(rowBuf, fieldLength);
  }

  public float decodeFloat(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeFloatText(rowBuf, fieldLength);
  }

  public double decodeDouble(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      MutableInt fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeDoubleText(rowBuf, fieldLength);
  }

  public boolean wasNull(byte[] nullBitmap, MutableInt fieldIndex, MutableInt fieldLength) {
    return fieldLength.get() == NULL_LENGTH;
  }

  /**
   * Set length and pos indicator to asked index.
   *
   * @param newIndex index (1 is first).
   */
  @Override
  public int setPosition(
      int newIndex,
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
