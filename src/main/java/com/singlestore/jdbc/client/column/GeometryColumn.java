// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client.column;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.util.MutableInt;
import com.singlestore.jdbc.type.LineString;
import com.singlestore.jdbc.type.Point;
import com.singlestore.jdbc.type.Polygon;
import java.sql.SQLDataException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

/** Column metadata definition */
public class GeometryColumn extends BlobColumn {

  /**
   * Geometry metadata type decoder
   *
   * @param buf buffer
   * @param charset charset
   * @param length maximum data length
   * @param dataType data type
   * @param decimals decimal length
   * @param flags flags
   * @param stringPos string offset position in buffer
   * @param extTypeName extended type name
   * @param extTypeFormat extended type format
   */
  public GeometryColumn(
      ReadableByteBuf buf,
      int charset,
      long length,
      DataType dataType,
      byte decimals,
      int flags,
      int[] stringPos,
      String extTypeName,
      String extTypeFormat) {
    super(buf, charset, length, dataType, decimals, flags, stringPos, extTypeName, extTypeFormat);
  }

  protected GeometryColumn(GeometryColumn prev) {
    super(prev);
  }

  @Override
  public GeometryColumn useAliasAsName() {
    return new GeometryColumn(this);
  }

  @Override
  public String defaultClassname(Configuration conf) {
    if (conf.geometryDefaultType() != null && "default".equals(conf.geometryDefaultType())) {
      if (extTypeName != null) {
        switch (extTypeName) {
          case "point":
            return Point.class.getName();
          case "linestring":
            return LineString.class.getName();
          case "polygon":
            return Polygon.class.getName();
        }
      }
    }
    return byte[].class.getName();
  }

  @Override
  public int getColumnType(Configuration conf) {
    return Types.VARBINARY;
  }

  @Override
  public String getColumnTypeName(Configuration conf) {
    return dataType.name();
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return getDefaultBinary(conf, buf, length);
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    if (dataType == DataType.CHAR) {
      String s = buf.readString(length.get());
      try {

        if (conf.geometryDefaultType() != null && "default".equals(conf.geometryDefaultType())) {
          if (extTypeName != null) {
            switch (extTypeName) {
              case "point":
                return new Point(s);
              case "linestring":
                return new LineString(s);
              case "polygon":
                return new Polygon(s);
            }
          }
        }
      } catch (IllegalArgumentException ex) {
        throw new SQLDataException(String.format("Failed to decode '%s' as %s", s, extTypeName));
      }
    }
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as %s", dataType, extTypeName));
  }

  @Override
  public Timestamp decodeTimestampText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Timestamp", dataType));
  }

  @Override
  public Timestamp decodeTimestampBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Timestamp", dataType));
  }
}
