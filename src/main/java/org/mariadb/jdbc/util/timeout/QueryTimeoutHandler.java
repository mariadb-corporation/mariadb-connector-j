// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.util.timeout;

/** Query Timeout handler for server that doesn't support query specific timeout */
public interface QueryTimeoutHandler extends AutoCloseable {
  QueryTimeoutHandler create(int queryTimeout);

  @Override
  void close();
}
