// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client;

import org.mariadb.jdbc.export.Prepare;
import org.mariadb.jdbc.message.server.CachedPrepareResultPacket;

/** LRU Prepare cache */
public interface PrepareCache {

  /**
   * Get cache value for key
   *
   * @param key key
   * @return Prepare value
   */
  CachedPrepareResultPacket get(String key);

  /**
   * Add a prepare cache value
   *
   * @param key key
   * @param result value
   * @return Prepare if was already cached
   */
  CachedPrepareResultPacket put(String key, Prepare result);

  /**
   * Increment execution count for a query and check if it should be promoted to server-side.
   *
   * @param key cache key
   * @return true if query has reached the threshold and should use server-side preparation
   */
  boolean incrementExecutionCount(String key);

  /**
   * Get the current execution count for a query.
   *
   * @param key cache key
   * @return execution count, or 0 if not tracked
   */
  int getExecutionCount(String key);

  /** Reset cache */
  void reset();
}
