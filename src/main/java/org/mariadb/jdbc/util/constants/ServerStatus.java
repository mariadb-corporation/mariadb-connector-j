// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

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
