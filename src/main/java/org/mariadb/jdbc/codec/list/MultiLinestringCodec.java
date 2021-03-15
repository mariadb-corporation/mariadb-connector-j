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
import java.sql.SQLDataException;
import java.util.Calendar;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.type.Geometry;
import org.mariadb.jdbc.type.LineString;
import org.mariadb.jdbc.type.MultiLineString;
import org.mariadb.jdbc.type.Point;

public class MultiLinestringCodec implements Codec<MultiLineString> {

  public static final MultiLinestringCodec INSTANCE = new MultiLinestringCodec();

  public String className() {
    return byte[].class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return column.getType() == DataType.GEOMETRY && type.isAssignableFrom(MultiLineString.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof MultiLineString;
  }

  @Override
  public MultiLineString decodeText(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    return decodeBinary(buf, length, column, cal);
  }

  @Override
  public MultiLineString decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    if (column.getType() == DataType.GEOMETRY) {
      buf.skip(4); // SRID
      Geometry geo = Geometry.getGeometry(buf, length - 4, column);
      if (geo instanceof MultiLineString) return (MultiLineString) geo;
      throw new SQLDataException(
          String.format(
              "Geometric type %s cannot be decoded as MultiLineString", geo.getClass().getName()));
    }
    buf.skip(length);
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as MultiLineString", column.getType()));
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeBytes(("MLineFromText('" + value.toString() + "')").getBytes());
  }

  @Override
  public void encodeBinary(
      PacketWriter encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException {
    MultiLineString mp = (MultiLineString) value;

    int length = 13;
    for (LineString ls : mp.getLines()) {
      length += 9 + ls.getPoints().length * 16;
    }

    encoder.writeLength(length);
    encoder.writeInt(0); // SRID
    encoder.writeByte(0x01); // LITTLE ENDIAN
    encoder.writeInt(5); // wkbMultiLineString
    encoder.writeInt(mp.getLines().length);
    for (LineString ls : mp.getLines()) {
      encoder.writeByte(0x01); // LITTLE ENDIAN
      encoder.writeInt(2); // wkbLineString
      encoder.writeInt(ls.getPoints().length); // nb points
      for (Point pt : ls.getPoints()) {
        encoder.writeDouble(pt.getX());
        encoder.writeDouble(pt.getY());
      }
    }
  }

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
