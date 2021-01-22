/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.codec.list;

import java.io.IOException;
import java.util.BitSet;
import java.util.Calendar;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class BitSetCodec implements Codec<BitSet> {

  public static final BitSetCodec INSTANCE = new BitSetCodec();

  public static BitSet parseBit(ReadableByteBuf buf, int length) {
    byte[] arr = new byte[length];
    buf.readBytes(arr);
    revertOrder(arr);
    return BitSet.valueOf(arr);
  }

  public static void revertOrder(byte[] array) {
    int i = 0;
    int j = array.length - 1;
    byte tmp;
    while (j > i) {
      tmp = array[j];
      array[j] = array[i];
      array[i] = tmp;
      j--;
      i++;
    }
  }

  public String className() {
    return BitSet.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return column.getType() == DataType.BIT && type.isAssignableFrom(BitSet.class);
  }

  @Override
  public BitSet decodeText(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal) {
    return parseBit(buf, length);
  }

  @Override
  public BitSet decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal) {
    return parseBit(buf, length);
  }

  public boolean canEncode(Object value) {
    return value instanceof BitSet;
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Object value, Calendar cal, Long length)
      throws IOException {
    byte[] bytes = ((BitSet)value).toByteArray();
    revertOrder(bytes);

    StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE + 3);
    sb.append("b'");
    for (int i = 0; i < Byte.SIZE * bytes.length; i++)
      sb.append((bytes[i / Byte.SIZE] << i % Byte.SIZE & 0x80) == 0 ? '0' : '1');
    sb.append("'");
    encoder.writeAscii(sb.toString());
  }

  @Override
  public void encodeBinary(PacketWriter encoder, Context context, Object value, Calendar cal)
      throws IOException {
    byte[] bytes = ((BitSet)value).toByteArray();
    revertOrder(bytes);
    encoder.writeLength(bytes.length);
    encoder.writeBytes(bytes);
  }

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
