// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab

package org.mariadb.jdbc.fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import java.util.Calendar;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.client.result.rowdecoder.BinaryRowDecoder;
import org.mariadb.jdbc.client.result.rowdecoder.RowDecoder;
import org.mariadb.jdbc.client.result.rowdecoder.TextRowDecoder;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.fuzz.support.FuzzContext;
import org.mariadb.jdbc.message.server.*;

/** Professional fuzzer for MariaDB protocol packets and row decoding. */
public class MariaDbDeepPacketFuzzer {
  private static final Calendar cal = Calendar.getInstance();
  private static final RowDecoder textRowDecoder = new TextRowDecoder();
  private static final RowDecoder binaryRowDecoder = new BinaryRowDecoder();

  public static void fuzzerTestOneInput(FuzzedDataProvider data) {
    Context context = new FuzzContext(data);
    byte[] bytes = data.consumeRemainingAsBytes();
    if (bytes.length < 1) return;

    StandardReadableByteBuf buf = new StandardReadableByteBuf(bytes);
    int choice = data.consumeInt(0, 15);

    try {
      switch (choice) {
        case 0:
          OkPacket.parse(buf, context);
          break;
        case 1:
          OkPacket.parseWithInfo(buf, context);
          break;
        case 2:
          new ErrorPacket(buf, context);
          break;
        case 3:
          AuthSwitchPacket.decode(buf);
          break;
        case 4:
          InitialHandshakePacket.decode(buf);
          break;
        case 5:
          ColumnDecoder decoder = ColumnDecoder.decode(buf);
          if (decoder != null) {
            fuzzDecoder(decoder, buf, data, context);
          }
          break;
        case 6:
          ColumnDecoder.decodeStd(buf);
          break;
        case 7:
          buf.readIntLengthEncodedNotNull();
          buf.readLongLengthEncodedNotNull();
          buf.readStringNullEnd();
          break;
        case 8:
          try {
            DataType.of(data.consumeByte() & 0xff);
          } catch (Exception e) {
          }
          break;
        case 9:
          fuzzRowDecoding(textRowDecoder, buf, data, context);
          break;
        case 10:
          fuzzRowDecoding(binaryRowDecoder, buf, data, context);
          break;
        case 11:
          try {
            new ColumnDefinitionPacket(
                buf, 33, 100, DataType.VARCHAR, (byte) 0, 0, new int[5], null, null, true);
          } catch (Exception e) {
          }
          break;
        case 12:
          // Reserved for future packet types
          break;
        case 13:
          buf.readAscii(data.consumeInt(1, 100));
          buf.readUnsignedInt();
          buf.readUnsignedShort();
          break;
        default:
          OkPacket.parse(buf, context);
          new ErrorPacket(buf, context);
          break;
      }
    } catch (Throwable e) {
      // Expected exceptions from malformed packets
    }
  }

  private static void fuzzRowDecoding(
      RowDecoder rowDecoder, StandardReadableByteBuf buf, FuzzedDataProvider data, Context context) {
    try {
      int colCount = data.consumeInt(1, 5);
      ColumnDecoder[] metadata = new ColumnDecoder[colCount];
      for (int i = 0; i < colCount; i++) {
        metadata[i] = ColumnDecoder.decodeStd(buf);
      }

      MutableInt fieldIndex = new MutableInt(-1);
      MutableInt fieldLength = new MutableInt(0);
      byte[] nullBitmap = new byte[(colCount + 9) / 8];

      int targetCol = data.consumeInt(0, colCount - 1);
      rowDecoder.setPosition(targetCol, fieldIndex, colCount, buf, nullBitmap, metadata);

      if (!rowDecoder.wasNull(nullBitmap, fieldIndex, fieldLength)) {
        int typeChoice = data.consumeInt(0, 10);
        switch (typeChoice) {
          case 0:
            rowDecoder.decodeString(metadata, fieldIndex, buf, fieldLength, context);
            break;
          case 1:
            rowDecoder.decodeInt(metadata, fieldIndex, buf, fieldLength);
            break;
          case 2:
            rowDecoder.decodeLong(metadata, fieldIndex, buf, fieldLength);
            break;
          case 3:
            rowDecoder.decodeDouble(metadata, fieldIndex, buf, fieldLength);
            break;
          case 4:
            rowDecoder.decodeFloat(metadata, fieldIndex, buf, fieldLength);
            break;
          case 5:
            rowDecoder.decodeShort(metadata, fieldIndex, buf, fieldLength);
            break;
          case 6:
            rowDecoder.decodeByte(metadata, fieldIndex, buf, fieldLength);
            break;
          case 7:
            rowDecoder.decodeBoolean(metadata, fieldIndex, buf, fieldLength);
            break;
          case 8:
            rowDecoder.decodeDate(metadata, fieldIndex, buf, fieldLength, cal, context);
            break;
          case 9:
            rowDecoder.decodeTime(metadata, fieldIndex, buf, fieldLength, cal, context);
            break;
          case 10:
            rowDecoder.decodeTimestamp(metadata, fieldIndex, buf, fieldLength, cal, context);
            break;
        }
      }
    } catch (Throwable e) {
    }
  }

  private static void fuzzDecoder(
      ColumnDecoder decoder, ReadableByteBuf buf, FuzzedDataProvider data, Context context) {
    try {
      MutableInt len = new MutableInt(0);
      int method = data.consumeInt(0, 10);
      switch (method) {
        case 0:
          decoder.decodeStringText(buf, len, cal, context);
          break;
        case 1:
          decoder.decodeIntText(buf, len);
          break;
        case 2:
          decoder.decodeDateText(buf, len, cal, context);
          break;
        case 3:
          decoder.decodeLongBinary(buf, len);
          break;
        case 4:
          decoder.getDefaultText(buf, len, context);
          break;
        case 5:
          decoder.decodeBooleanText(buf, len);
          break;
        case 6:
          decoder.decodeByteText(buf, len);
          break;
        case 7:
          decoder.decodeDoubleBinary(buf, len);
          break;
        case 8:
          decoder.decodeFloatBinary(buf, len);
          break;
        case 9:
          decoder.decodeShortBinary(buf, len);
          break;
        case 10:
          decoder.decodeTimestampBinary(buf, len, cal, context);
          break;
      }
    } catch (Throwable e) {
    }
  }
}
