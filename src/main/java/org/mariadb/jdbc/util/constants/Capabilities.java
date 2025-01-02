// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.util.constants;

/** Client / Server capabilities */
@SuppressWarnings("unused")
public final class Capabilities {

  /** Is client mysql */
  public static final int CLIENT_MYSQL = 1;

  /** use Found rowd instead of affected rows */
  public static final int FOUND_ROWS = 2;

  /** Get all column flags */
  public static final int LONG_FLAG = 4;

  /** One can specify db on connect */
  public static final int CONNECT_WITH_DB = 8;

  /** Don't allow database.table.column */
  public static final int NO_SCHEMA = 16;

  /** use compression protocol */
  public static final int COMPRESS = 32;

  /** Odbc client */
  public static final int ODBC = 64;

  /** Can use LOAD DATA LOCAL */
  public static final int LOCAL_FILES = 128;

  /** Ignore spaces before '(' */
  public static final int IGNORE_SPACE = 256;

  /** Use 4.1 protocol */
  public static final int CLIENT_PROTOCOL_41 = 512;

  /** Is interactive client */
  public static final int CLIENT_INTERACTIVE = 1024;

  /** Switch to SSL after handshake */
  public static final int SSL = 2048;

  /** IGNORE sigpipes */
  public static final int IGNORE_SIGPIPE = 4096;

  /** transactions */
  public static final int TRANSACTIONS = 8192;

  /** reserved - not used */
  public static final int RESERVED = 16384;

  /** New 4.1 authentication */
  public static final int SECURE_CONNECTION = 32768;

  /** Enable/disable multi-stmt support */
  public static final int MULTI_STATEMENTS = 1 << 16;

  /** Enable/disable multi-results */
  public static final int MULTI_RESULTS = 1 << 17;

  /** Enable/disable multi-results for PrepareStatement */
  public static final int PS_MULTI_RESULTS = 1 << 18;

  /** Client supports plugin authentication */
  public static final int PLUGIN_AUTH = 1 << 19;

  /** Client send connection attributes */
  public static final int CONNECT_ATTRS = 1 << 20;

  /** authentication data length is a length auth integer */
  public static final int PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 << 21;

  /** server send session tracking info */
  public static final int CLIENT_SESSION_TRACK = 1 << 23;

  /** EOF packet deprecated */
  public static final int CLIENT_DEPRECATE_EOF = 1 << 24;

  /** Client support progress indicator (before 10.2) */
  public static final int PROGRESS_OLD = 1 << 29;

  /* MariaDB specific capabilities */

  /** Client progression */
  public static final long PROGRESS = 1L << 32;

  /** not used anymore - reserved */
  public static final long MARIADB_RESERVED = 1L << 33;

  /** permit COM_STMT_BULK commands */
  public static final long STMT_BULK_OPERATIONS = 1L << 34;

  /** metadata extended information */
  public static final long EXTENDED_METADATA = 1L << 35;

  /** permit metadata caching */
  public static final long CACHE_METADATA = 1L << 36;

  /** permit returning all bulk individual results */
  public static final long BULK_UNIT_RESULTS = 1L << 37;
}
