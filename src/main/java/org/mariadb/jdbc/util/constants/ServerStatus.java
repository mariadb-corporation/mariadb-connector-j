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

public class ServerStatus {

  public static final short IN_TRANSACTION = 1;
  public static final short AUTOCOMMIT = 2;
  public static final short MORE_RESULTS_EXISTS = 8;
  public static final short QUERY_NO_GOOD_INDEX_USED = 16;
  public static final short QUERY_NO_INDEX_USED = 32;
  public static final short CURSOR_EXISTS = 64;
  public static final short LAST_ROW_SENT = 128;
  public static final short DB_DROPPED = 256;
  public static final short NO_BACKSLASH_ESCAPES = 512;
  public static final short METADATA_CHANGED = 1024;
  public static final short QUERY_WAS_SLOW = 2048;
  public static final short PS_OUT_PARAMETERS = 4096;
  public static final short SERVER_SESSION_STATE_CHANGED = 1 << 14;
}
