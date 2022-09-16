package org.mariadb.jdbc.client.result.rowdecoder;

import static org.mariadb.jdbc.client.result.Result.NULL_LENGTH;

import java.sql.SQLException;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

public class TextRowDecoder implements RowDecoder {

  @Override
  public <T> T decode(
      Codec<T> codec,
      Calendar cal,
      StandardReadableByteBuf rowBuf,
      int fieldLength,
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
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].getDefaultText(conf, rowBuf, fieldLength);
  }

  @Override
  public String decodeString(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeStringText(rowBuf, fieldLength, null);
  }

  public byte decodeByte(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeByteText(rowBuf, fieldLength);
  }

  public boolean decodeBoolean(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeBooleanText(rowBuf, fieldLength);
  }

  public short decodeShort(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeShortText(rowBuf, fieldLength);
  }

  public int decodeInt(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeIntText(rowBuf, fieldLength);
  }

  public long decodeLong(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeLongText(rowBuf, fieldLength);
  }

  public float decodeFloat(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeFloatText(rowBuf, fieldLength);
  }

  public double decodeDouble(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeDoubleText(rowBuf, fieldLength);
  }

  public boolean wasNull(byte[] nullBitmap, MutableInt fieldIndex, int fieldLength) {
    return fieldLength == NULL_LENGTH;
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
