// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.message.server;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mariadb.jdbc.BasePreparedStatement;
import org.mariadb.jdbc.client.Client;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;

/** Prepare packet result with flag indicating use */
public final class CachedPrepareResultPacket extends PrepareResultPacket {

  private final AtomicBoolean closing = new AtomicBoolean();
  private final AtomicBoolean cached = new AtomicBoolean();
  private final List<BasePreparedStatement> statements = new ArrayList<>();

  /**
   * Cache prepare result with flag indicating use
   *
   * @param buffer prepare packet buffer
   * @param reader packet reader
   * @param context connection context
   * @throws IOException if any socket error occurs
   */
  public CachedPrepareResultPacket(ReadableByteBuf buffer, Reader reader, Context context)
      throws IOException {
    super(buffer, reader, context);
  }

  /**
   * Indicate that a prepare statement must be closed (if not in LRU cache)
   *
   * @param con current connection
   * @throws SQLException if SQL
   */
  public void close(Client con) throws SQLException {
    if (!cached.get() && closing.compareAndSet(false, true)) {
      con.closePrepare(this);
    }
  }

  public void decrementUse(Client con, BasePreparedStatement preparedStatement)
      throws SQLException {
    statements.remove(preparedStatement);
    if (statements.isEmpty() && !cached.get()) {
      close(con);
    }
  }

  /**
   * Increment use of prepare statement.
   *
   * @param preparedStatement new statement using prepare result
   */
  public void incrementUse(BasePreparedStatement preparedStatement) {
    if (closing.get()) {
      return;
    }
    if (preparedStatement != null) statements.add(preparedStatement);
  }

  /**
   * Indicate that Prepare command is not on LRU cache anymore. closing prepare command if not used
   *
   * @param con current connection
   */
  public void unCache(Client con) {
    cached.set(false);
    if (statements.size() <= 0) {
      try {
        close(con);
      } catch (SQLException e) {
        // eat
      }
    }
  }

  /**
   * indicate that result is in LRU cache
   *
   * @return true if cached
   */
  public boolean cache() {
    if (closing.get()) {
      return false;
    }
    return cached.compareAndSet(false, true);
  }

  /**
   * Return prepare statement id.
   *
   * @return statement id
   */
  public int getStatementId() {
    return statementId;
  }

  /** Resetting cache in case of failover */
  public void reset() {
    statementId = -1;
    for (BasePreparedStatement stmt : statements) {
      stmt.reset();
    }
  }
}
