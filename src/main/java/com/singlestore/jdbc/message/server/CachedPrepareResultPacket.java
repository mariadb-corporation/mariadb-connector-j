// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.server;

import com.singlestore.jdbc.ServerPreparedStatement;
import com.singlestore.jdbc.client.Client;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.socket.Reader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public final class CachedPrepareResultPacket extends PrepareResultPacket {

  private final AtomicBoolean closing = new AtomicBoolean();
  private final AtomicBoolean cached = new AtomicBoolean();
  private final List<ServerPreparedStatement> statements = new ArrayList<>();

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

  public void decrementUse(Client con, ServerPreparedStatement preparedStatement)
      throws SQLException {
    statements.remove(preparedStatement);
    if (statements.size() == 0 && !cached.get()) {
      close(con);
    }
  }

  /**
   * Increment use of prepare statement.
   *
   * @param preparedStatement new statement using prepare result
   */
  public void incrementUse(ServerPreparedStatement preparedStatement) {
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

  public void reset() {
    statementId = -1;
    for (ServerPreparedStatement stmt : statements) {
      stmt.reset();
    }
  }
}
