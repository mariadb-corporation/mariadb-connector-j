// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.pool;

import java.util.concurrent.atomic.AtomicLong;
import javax.sql.*;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.MariaDbPoolConnection;

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

  public void close() {
    fireConnectionClosed(new ConnectionEvent(this));
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

  public void ensureValidation() {
    lastUsed.set(0L);
  }
}
