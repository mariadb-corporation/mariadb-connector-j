// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client;

import com.singlestore.jdbc.ServerPreparedStatement;
import com.singlestore.jdbc.export.Prepare;

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
