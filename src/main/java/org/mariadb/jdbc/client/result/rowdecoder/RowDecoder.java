package org.mariadb.jdbc.client.result.rowdecoder;

import java.sql.SQLException;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

public interface RowDecoder {

  boolean wasNull(byte[] nullBitmap, MutableInt fieldIndex, int fieldLength);

  int setPosition(
      int newIndex,
      MutableInt fieldIndex,
      int maxIndex,
      StandardReadableByteBuf rowBuf,
      byte[] nullBitmap,
      ColumnDecoder[] metadataList);

  <T> T decode(
      Codec<T> codec,
      Calendar calendar,
      StandardReadableByteBuf rowBuf,
      int fieldLength,
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex)
      throws SQLException;

  Object defaultDecode(
      Configuration conf,
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException;

  byte decodeByte(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException;

  boolean decodeBoolean(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException;

  short decodeShort(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException;

  int decodeInt(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException;

  String decodeString(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException;

  long decodeLong(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException;

  float decodeFloat(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException;

  double decodeDouble(
      ColumnDecoder[] metadataList,
      MutableInt fieldIndex,
      StandardReadableByteBuf rowBuf,
      int fieldLength)
      throws SQLException;
}
