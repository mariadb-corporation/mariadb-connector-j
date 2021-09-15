// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.codec;

public enum DataType {
  OLDDECIMAL(0),
  TINYINT(1),
  SMALLINT(2),
  INTEGER(3),
  FLOAT(4),
  DOUBLE(5),
  NULL(6),
  TIMESTAMP(7),
  BIGINT(8),
  MEDIUMINT(9),
  DATE(10),
  TIME(11),
  DATETIME(12),
  YEAR(13),
  NEWDATE(14),
  VARCHAR(15),
  BIT(16),
  JSON(245),
  DECIMAL(246),
  ENUM(247),
  SET(248),
  TINYBLOB(249),
  MEDIUMBLOB(250),
  LONGBLOB(251),
  BLOB(252),
  VARSTRING(253),
  STRING(254),
  GEOMETRY(255);

  static final DataType[] typeMap;

  static {
    typeMap = new DataType[256];
    for (DataType v : values()) {
      typeMap[v.mariadbType] = v;
    }
  }

  private final int mariadbType;

  DataType(int mariadbType) {
    this.mariadbType = mariadbType;
  }

  public int get() {
    return mariadbType;
  }

  public static DataType of(int typeValue) {
    return typeMap[typeValue];
  }
}
