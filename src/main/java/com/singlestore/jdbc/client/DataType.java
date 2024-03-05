// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.client;

import com.singlestore.jdbc.client.column.BigDecimalColumn;
import com.singlestore.jdbc.client.column.BitColumn;
import com.singlestore.jdbc.client.column.BlobColumn;
import com.singlestore.jdbc.client.column.DateColumn;
import com.singlestore.jdbc.client.column.DoubleColumn;
import com.singlestore.jdbc.client.column.FloatColumn;
import com.singlestore.jdbc.client.column.GeometryColumn;
import com.singlestore.jdbc.client.column.JsonColumn;
import com.singlestore.jdbc.client.column.SignedBigIntColumn;
import com.singlestore.jdbc.client.column.SignedIntColumn;
import com.singlestore.jdbc.client.column.SignedMediumIntColumn;
import com.singlestore.jdbc.client.column.SignedSmallIntColumn;
import com.singlestore.jdbc.client.column.SignedTinyIntColumn;
import com.singlestore.jdbc.client.column.StringColumn;
import com.singlestore.jdbc.client.column.TimeColumn;
import com.singlestore.jdbc.client.column.TimestampColumn;
import com.singlestore.jdbc.client.column.UnsignedBigIntColumn;
import com.singlestore.jdbc.client.column.UnsignedIntColumn;
import com.singlestore.jdbc.client.column.UnsignedMediumIntColumn;
import com.singlestore.jdbc.client.column.UnsignedSmallIntColumn;
import com.singlestore.jdbc.client.column.UnsignedTinyIntColumn;
import com.singlestore.jdbc.client.column.YearColumn;

public enum DataType {
  OLDDECIMAL(0, BigDecimalColumn::new, BigDecimalColumn::new),
  TINYINT(1, SignedTinyIntColumn::new, UnsignedTinyIntColumn::new),
  SMALLINT(2, SignedSmallIntColumn::new, UnsignedSmallIntColumn::new),
  INT(3, SignedIntColumn::new, UnsignedIntColumn::new),
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
  BIT(16, BitColumn::new, BitColumn::new),
  JSON(245, JsonColumn::new, JsonColumn::new),
  DECIMAL(246, BigDecimalColumn::new, BigDecimalColumn::new),
  ENUM(247, StringColumn::new, StringColumn::new),
  SET(248, StringColumn::new, StringColumn::new),
  TINYBLOB(249, BlobColumn::new, BlobColumn::new),
  MEDIUMBLOB(250, BlobColumn::new, BlobColumn::new),
  LONGBLOB(251, BlobColumn::new, BlobColumn::new),
  BLOB(252, BlobColumn::new, BlobColumn::new),
  VARCHAR(253, StringColumn::new, StringColumn::new),
  CHAR(254, StringColumn::new, StringColumn::new),
  GEOMETRY(255, GeometryColumn::new, GeometryColumn::new);

  static final DataType[] typeMap;

  static {
    typeMap = new DataType[256];
    for (DataType v : values()) {
      typeMap[v.singlestoreType] = v;
    }
  }

  private final int singlestoreType;
  private final ColumnConstructor columnConstructor;
  private final ColumnConstructor unsignedColumnConstructor;

  DataType(
      int singlestoreType,
      ColumnConstructor columnConstructor,
      ColumnConstructor unsignedColumnConstructor) {
    this.singlestoreType = singlestoreType;
    this.columnConstructor = columnConstructor;
    this.unsignedColumnConstructor = unsignedColumnConstructor;
  }

  public int get() {
    return singlestoreType;
  }

  public static DataType of(int typeValue) {
    return typeMap[typeValue];
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
