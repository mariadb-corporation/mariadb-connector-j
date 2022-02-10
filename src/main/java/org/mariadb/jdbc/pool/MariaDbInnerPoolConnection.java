// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.pool;

import java.util.concurrent.atomic.AtomicLong;
import javax.sql.*;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.MariaDbPoolConnection;

/**
 * MariaDB pool connection for internal pool permit to add a last used information, to remove
 * connection after staying in pool for long time.
 */
public class MariaDbInnerPoolConnection extends MariaDbPoolConnection {
  private final AtomicLong lastUsed;

  /**
   * Constructor.
   *
   * @param connection connection to retrieve connection options
   */
  public MariaDbInnerPoolConnection(Connection connection) {
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

  /** Reset last used time, to ensure next retrieval will validate connection before borrowing */
  public void ensureValidation() {
    lastUsed.set(0L);
  }
}
