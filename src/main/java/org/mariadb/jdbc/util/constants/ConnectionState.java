// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.util.constants;

/** Connection state change, to ensure what is needed to reset connection to a proper state */
public final class ConnectionState {
  /** flag indicating that network timeout has been changed */
  public static final int STATE_NETWORK_TIMEOUT = 1;

  /** flag indicating that default database has been changed */
  public static final int STATE_DATABASE = 2;

  /** flag indicating that connection read only has been changed */
  public static final int STATE_READ_ONLY = 4;

  /** flag indicating that autocommit has been changed */
  public static final int STATE_AUTOCOMMIT = 8;

  /** flag indicating that transaction isolation has been changed */
  public static final int STATE_TRANSACTION_ISOLATION = 16;
}
