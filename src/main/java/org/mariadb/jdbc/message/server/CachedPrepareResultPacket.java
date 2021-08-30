// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.server;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.client.Client;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;

public final class CachedPrepareResultPacket extends PrepareResultPacket {

  private final AtomicBoolean closing = new AtomicBoolean();
  private final AtomicBoolean cached = new AtomicBoolean();
  private final List<ServerPreparedStatement> statements = new ArrayList<>();

  public CachedPrepareResultPacket(ReadableByteBuf buffer, Reader reader, Context context)
      throws IOException {
    super(buffer, reader, context);
  }

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

  public void incrementUse(ServerPreparedStatement preparedStatement) {
    if (closing.get()) {
      return;
    }
    if (preparedStatement != null) statements.add(preparedStatement);
  }

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

  public boolean cache() {
    if (closing.get()) {
      return false;
    }
    return cached.compareAndSet(false, true);
  }

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
