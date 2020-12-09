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

@SuppressWarnings("unused")
public class Capabilities {

  public static final int CLIENT_MYSQL = 1;
  public static final int FOUND_ROWS = 2; /* Found instead of affected rows */
  public static final int LONG_FLAG = 4; /* Get all column flags */
  public static final int CONNECT_WITH_DB = 8; /* One can specify db on connect */
  public static final int NO_SCHEMA = 16; /* Don't allow database.table.column */
  public static final int COMPRESS = 32; /* Can use compression protocol */
  public static final int ODBC = 64; /* Odbc client */
  public static final int LOCAL_FILES = 128; /* Can use LOAD DATA LOCAL */
  public static final int IGNORE_SPACE = 256; /* Ignore spaces before '(' */
  public static final int CLIENT_PROTOCOL_41 = 512; /* New 4.1 protocol */
  public static final int CLIENT_INTERACTIVE = 1024;
  public static final int SSL = 2048; /* Switch to SSL after handshake */
  public static final int IGNORE_SIGPIPE = 4096; /* IGNORE sigpipes */
  public static final int TRANSACTIONS = 8192;
  public static final int RESERVED = 16384; /* Old flag for 4.1 protocol  */
  public static final int SECURE_CONNECTION = 32768; /* New 4.1 authentication */
  public static final int MULTI_STATEMENTS = 1 << 16; /* Enable/disable multi-stmt support */
  public static final int MULTI_RESULTS = 1 << 17; /* Enable/disable multi-results */
  public static final int PS_MULTI_RESULTS =
      1 << 18; /* Enable/disable multi-results for PrepareStatement */
  public static final int PLUGIN_AUTH = 1 << 19; /* Client supports plugin authentication */
  public static final int CONNECT_ATTRS = 1 << 20; /* Client send connection attributes */
  public static final int PLUGIN_AUTH_LENENC_CLIENT_DATA =
      1 << 21; /* authentication data length is a length auth integer */
  public static final int CLIENT_SESSION_TRACK = 1 << 23; /* server send session tracking info */
  public static final int CLIENT_DEPRECATE_EOF = 1 << 24; /* EOF packet deprecated */
  public static final int PROGRESS_OLD =
      1 << 29; /* Client support progress indicator (before 10.2)*/

  /* MariaDB specific capabilities */
  public static final long MARIADB_CLIENT_PROGRESS = 1L << 32;
  public static final long MARIADB_CLIENT_COM_MULTI = 1L << 33;
  public static final long MARIADB_CLIENT_STMT_BULK_OPERATIONS = 1L << 34;
  public static final long MARIADB_CLIENT_EXTENDED_TYPE_INFO = 1L << 35;
}
