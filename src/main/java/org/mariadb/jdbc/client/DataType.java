// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.client;

import org.mariadb.jdbc.client.column.*;

public enum DataType {
  OLDDECIMAL(0, BigDecimalColumn::new, BigDecimalColumn::new),
  TINYINT(1, SignedTinyIntColumn::new, UnsignedTinyIntColumn::new),
  SMALLINT(2, SignedSmallIntColumn::new, UnsignedSmallIntColumn::new),
  INTEGER(3, SignedIntColumn::new, UnsignedIntColumn::new),
  FLOAT(4, FloatColumn::new, FloatColumn::new),
  DOUBLE(5, DoubleColumn::new, DoubleColumn::new),
  NULL(6, StringColumn::new, StringColumn::new),
  TIMESTAMP(7, TimestampColumn::new, TimestampColumn::new),
  BIGINT(8, SignedBigIntColumn::new, UnsignedBigIntColumn::new),
  MEDIUMINT(9, SignedMediumIntColumn::new, UnsignedMediumIntColumn::new),
  DATE(10, DateColumn::new, DateColumn::new),
  TIME(11, TimeColumn::new, TimeColumn::new),
  DATETIME(12, TimestampColumn::new, TimestampColumn::new),
  YEAR(13, YearColumn::new, YearColumn::new),
  NEWDATE(14, DateColumn::new, DateColumn::new),
  VARCHAR(15, StringColumn::new, StringColumn::new),
  BIT(16, BitColumn::new, BitColumn::new),
  JSON(245, JsonColumn::new, JsonColumn::new),
  DECIMAL(246, BigDecimalColumn::new, BigDecimalColumn::new),
  ENUM(247, StringColumn::new, StringColumn::new),
  SET(248, StringColumn::new, StringColumn::new),
  TINYBLOB(249, BlobColumn::new, BlobColumn::new),
  MEDIUMBLOB(250, BlobColumn::new, BlobColumn::new),
  LONGBLOB(251, BlobColumn::new, BlobColumn::new),
  BLOB(252, BlobColumn::new, BlobColumn::new),
  VARSTRING(253, StringColumn::new, StringColumn::new),
  STRING(254, StringColumn::new, StringColumn::new),
  GEOMETRY(255, GeometryColumn::new, GeometryColumn::new);

  static final DataType[] typeMap;

  static {
    typeMap = new DataType[256];
    for (DataType v : values()) {
      typeMap[v.mariadbType] = v;
    }
  }

  private final int mariadbType;
  private final ColumnConstructor columnConstructor;
  private final ColumnConstructor unsignedColumnConstructor;

  DataType(
      int mariadbType,
      ColumnConstructor columnConstructor,
      ColumnConstructor unsignedColumnConstructor) {
    this.mariadbType = mariadbType;
    this.columnConstructor = columnConstructor;
    this.unsignedColumnConstructor = unsignedColumnConstructor;
  }

  public static DataType of(int typeValue) {
    return typeMap[typeValue];
  }

  public int get() {
    return mariadbType;
  }

  public ColumnConstructor getColumnConstructor() {
    return columnConstructor;
  }

  public ColumnConstructor getUnsignedColumnConstructor() {
    return unsignedColumnConstructor;
  }

  @FunctionalInterface
  public interface ColumnConstructor {

    ColumnDecoder create(
        ReadableByteBuf buf,
        int charset,
        long length,
        DataType dataType,
        byte decimals,
        int flags,
        int[] stringPos,
        String extTypeName,
        String extTypeFormat);
  }
}
