// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.util.constants;

/** Column flag see https://mariadb.com/kb/en/result-set-packets/#field-details-flag */
@SuppressWarnings("unused")
public final class ColumnFlags {

  /** must a column have non-null value only */
  public static final short NOT_NULL = 1;

  /** Is column a primary key */
  public static final short PRIMARY_KEY = 2;

  /** Is this column a unique key */
  public static final short UNIQUE_KEY = 4;

  /** Is this column part of a multiple column key */
  public static final short MULTIPLE_KEY = 8;

  /** Does this column contain blob */
  public static final short BLOB = 16;

  /** Is column number value unsigned */
  public static final short UNSIGNED = 32;

  /** Must number value be filled with Zero */
  public static final short ZEROFILL = 64;

  /** Is binary value */
  public static final short BINARY_COLLATION = 128;

  /** Is column of type enum */
  public static final short ENUM = 256;

  /** Does column auto-increment */
  public static final short AUTO_INCREMENT = 512;

  /** Is column of type Timestamp */
  public static final short TIMESTAMP = 1024;

  /** Is column type set */
  public static final short SET = 2048;

  /** Does column have no default value */
  public static final short NO_DEFAULT_VALUE_FLAG = 4096;
}
