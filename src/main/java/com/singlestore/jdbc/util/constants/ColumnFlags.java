// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.util.constants;

@SuppressWarnings("unused")
public final class ColumnFlags {
  public static final short NOT_NULL = 1;
  public static final short PRIMARY_KEY = 2;
  public static final short UNIQUE_KEY = 4;
  public static final short MULTIPLE_KEY = 8;
  public static final short BLOB = 16;
  public static final short UNSIGNED = 32;
  public static final short ZEROFILL = 64;
  public static final short BINARY_COLLATION = 128;
  public static final short ENUM = 256;
  public static final short AUTO_INCREMENT = 512;
  public static final short TIMESTAMP = 1024;
  public static final short SET = 2048;
  public static final short NO_DEFAULT_VALUE_FLAG = 4096;
}
