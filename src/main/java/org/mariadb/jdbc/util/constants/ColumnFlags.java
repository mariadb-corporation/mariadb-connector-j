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

package org.mariadb.jdbc.util.constants;

public class ColumnFlags {
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
