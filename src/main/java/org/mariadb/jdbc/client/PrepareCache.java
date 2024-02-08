// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.client;

import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.export.Prepare;

/** LRU Prepare cache */
public interface PrepareCache {

  /**
   * Get cache value for key
   *
   * @param key key
   * @param preparedStatement prepared statement
   * @return Prepare value
   */
  Prepare get(String key, ServerPreparedStatement preparedStatement);

  /**
   * Add a prepare cache value
   *
   * @param key key
   * @param result value
   * @param preparedStatement prepared statement
   * @return Prepare if was already cached
   */
  Prepare put(String key, Prepare result, ServerPreparedStatement preparedStatement);

  /** Reset cache */
  void reset();
}
