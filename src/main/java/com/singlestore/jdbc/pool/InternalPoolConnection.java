// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.pool;

import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.SingleStorePoolConnection;
import java.util.concurrent.atomic.AtomicLong;

public class InternalPoolConnection extends SingleStorePoolConnection {
  private final AtomicLong lastUsed;
  private boolean failed;

  /**
   * Constructor.
   *
   * @param connection connection to retrieve connection options
   */
  public InternalPoolConnection(Connection connection) {
    super(connection);
    lastUsed = new AtomicLong(System.nanoTime());
  }

  /**
   * Indicate last time this pool connection has been used.
   *
   * @return current last used time (nano).
   */
  public AtomicLong getLastUsed() {
    return lastUsed;
  }

  /** Set last poolConnection use to now. */
  public void lastUsedToNow() {
    lastUsed.set(System.nanoTime());
  }

  public boolean isFailed() {
    return failed;
  }

  public void setFailed(boolean failed) {
    this.failed = failed;
  }

  /** Reset last used time, to ensure next retrieval will validate connection before borrowing */
  public void ensureValidation() {
    lastUsed.set(0L);
  }
}
