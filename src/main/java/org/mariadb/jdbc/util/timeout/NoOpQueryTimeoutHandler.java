// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.util.timeout;

/** Default no op query timeout. */
public class NoOpQueryTimeoutHandler implements QueryTimeoutHandler {
  public static final NoOpQueryTimeoutHandler INSTANCE = new NoOpQueryTimeoutHandler();

  @Override
  public QueryTimeoutHandler create(int queryTimeout) {
    return INSTANCE;
  }

  @Override
  public void close() {}
}
