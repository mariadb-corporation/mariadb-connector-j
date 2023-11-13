// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import java.sql.*;
import java.util.Calendar;
import java.util.Locale;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.codec.*;
import org.mariadb.jdbc.type.*;

/** Column metadata definition */
public class GeometryColumn extends BlobColumn {

  /**
   * Geometry metadata type decoder
   *
   * @param buf buffer
   * @param charset charset
   * @param length maximum data length
   * @param dataType data type. see https://mariadb.com/kb/en/result-set-packets/#field-types
   * @param decimals decimal length
   * @param flags flags. see https://mariadb.com/kb/en/result-set-packets/#field-details-flag
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
          case "multipoint":
            return MultiPoint.class.getName();
          case "multilinestring":
            return MultiLineString.class.getName();
          case "multipolygon":
            return MultiPolygon.class.getName();
          case "geometrycollection":
            return GeometryCollection.class.getName();
        }
      }
      return GeometryCollection.class.getName();
    }
    return "byte[]";
  }

  public int getColumnType(Configuration conf) {
    return Types.VARBINARY;
  }

  public String getColumnTypeName(Configuration conf) {
    if (extTypeName != null) {
      return extTypeName.toUpperCase(Locale.ROOT);
    }
    return "GEOMETRY";
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    if (conf.geometryDefaultType() != null && "default".equals(conf.geometryDefaultType())) {
      buf.skip(4); // SRID
      return Geometry.getGeometry(buf, length.get() - 4, this);
    }
    byte[] arr = new byte[length.get()];
    buf.readBytes(arr);
    return arr;
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return getDefaultText(conf, buf, length);
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
