// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client;

import org.mariadb.jdbc.client.column.*;

public enum DataType {
  OLDDECIMAL(0, BigDecimalColumn::new),
  TINYINT(1, TinyIntColumn::new),
  SMALLINT(2, SmallIntColumn::new),
  INTEGER(3, IntColumn::new),
  FLOAT(4, FloatColumn::new),
  DOUBLE(5, DoubleColumn::new),
  NULL(6, StringColumn::new),
  TIMESTAMP(7, TimestampColumn::new),
  BIGINT(8, BigIntColumn::new),
  MEDIUMINT(9, MediumIntColumn::new),
  DATE(10, DateColumn::new),
  TIME(11, TimeColumn::new),
  DATETIME(12, TimestampColumn::new),
  YEAR(13, YearColumn::new),
  NEWDATE(14, DateColumn::new),
  VARCHAR(15, StringColumn::new),
  BIT(16, BitColumn::new),
  JSON(245, StringColumn::new),
  DECIMAL(246, BigDecimalColumn::new),
  ENUM(247, StringColumn::new),
  SET(248, StringColumn::new),
  TINYBLOB(249, BlobColumn::new),
  MEDIUMBLOB(250, BlobColumn::new),
  LONGBLOB(251, BlobColumn::new),
  BLOB(252, BlobColumn::new),
  VARSTRING(253, StringColumn::new),
  STRING(254, StringColumn::new),
  GEOMETRY(255, GeometryColumn::new);

  static final DataType[] typeMap;

  static {
    typeMap = new DataType[256];
    for (DataType v : values()) {
      typeMap[v.mariadbType] = v;
    }
  }

  private final int mariadbType;
  private final ColumnConstructor columnConstructor;

  DataType(int mariadbType, ColumnConstructor columnConstructor) {
    this.mariadbType = mariadbType;
    this.columnConstructor = columnConstructor;
  }

  public int get() {
    return mariadbType;
  }

  public static DataType of(int typeValue) {
    return typeMap[typeValue];
  }

  public ColumnConstructor getColumnConstructor() {
    return columnConstructor;
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
