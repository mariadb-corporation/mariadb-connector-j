package org.mariadb.jdbc.client.result.rowdecoder;

import static org.mariadb.jdbc.client.result.Result.NULL_LENGTH;

import java.sql.SQLException;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

public class BinaryRowDecoder implements RowDecoder {

  public <T> T decode(
      Codec<T> codec,
      Calendar cal,
      StandardReadableByteBuf rowBuf,
      int fieldLength,
      ColumnDecoder[] metadataList,
      final MutableInt fieldIndex)
      throws SQLException {
    return codec.decodeBinary(rowBuf, fieldLength, metadataList[fieldIndex.get()], cal);
  }

  @Override
  public Object defaultDecode(
      Configuration conf,
      ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].getDefaultBinary(conf, rowBuf, fieldLength);
  }

  public String decodeString(
      ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeStringBinary(rowBuf, fieldLength, null);
  }

  public byte decodeByte(
      ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeByteBinary(rowBuf, fieldLength);
  }

  public boolean decodeBoolean(
      ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeBooleanBinary(rowBuf, fieldLength);
  }

  public short decodeShort(
      ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeShortBinary(rowBuf, fieldLength);
  }

  public int decodeInt(
      ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeIntBinary(rowBuf, fieldLength);
  }

  public long decodeLong(
      ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeLongBinary(rowBuf, fieldLength);
  }

  public float decodeFloat(
      ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeFloatBinary(rowBuf, fieldLength);
  }

  public double decodeDouble(
      ColumnDecoder[] metadataList,
      final MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException {
    return metadataList[fieldIndex.get()].decodeDoubleBinary(rowBuf, fieldLength);
  }

  public boolean wasNull(byte[] nullBitmap, final MutableInt fieldIndex, int fieldLength) {
    return (nullBitmap[(fieldIndex.get() + 2) / 8] & (1 << ((fieldIndex.get() + 2) % 8))) > 0;
  }

  /**
   * Set length and pos indicator to asked index.
   *
   * @param newIndex index (0 is first).
   */
  @Override
  public int setPosition(
      int newIndex,
      final MutableInt fieldIndex,
      int maxIndex,
      StandardReadableByteBuf rowBuf,
      byte[] nullBitmap,
      ColumnDecoder[] metadataList) {

    if (fieldIndex.get() >= newIndex) {
      fieldIndex.set(0);
      // skip header + null-bitmap
      rowBuf.pos(1 + ((maxIndex + 9) / 8));
    } else {
      fieldIndex.incrementAndGet();
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

    if (wasNull(nullBitmap, fieldIndex, 0)) {
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
