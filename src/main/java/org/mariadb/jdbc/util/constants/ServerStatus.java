// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.util.constants;

/** Server status flags */
@SuppressWarnings("unused")
public class ServerStatus {
  /** is in transaction */
  public static final short IN_TRANSACTION = 1;

  /** autocommit */
  public static final short AUTOCOMMIT = 2;

  /** more result exists (packet follows) */
  public static final short MORE_RESULTS_EXISTS = 8;

  /** no good index was used */
  public static final short QUERY_NO_GOOD_INDEX_USED = 16;

  /** no index was used */
  public static final short QUERY_NO_INDEX_USED = 32;

  /** cursor exists */
  public static final short CURSOR_EXISTS = 64;

  /** last row sent */
  public static final short LAST_ROW_SENT = 128;

  /** database dropped */
  public static final short DB_DROPPED = 256;

  /** escape type */
  public static final short NO_BACKSLASH_ESCAPES = 512;

  /** metadata changed */
  public static final short METADATA_CHANGED = 1024;

  /** query was slow */
  public static final short QUERY_WAS_SLOW = 2048;

  /** resultset contains output parameters */
  public static final short PS_OUT_PARAMETERS = 4096;

  /** session state change (OK_Packet contains additional data) */
  public static final short SERVER_SESSION_STATE_CHANGED = 1 << 14;
}
