// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.util.timeout;

import java.util.concurrent.*;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.client.util.ClosableLock;
import org.mariadb.jdbc.client.util.SchedulerProvider;

public class QueryTimeoutHandlerImpl implements QueryTimeoutHandler {
  private Future<?> timerTaskFuture;
  private Connection conn;

  public QueryTimeoutHandler create(int queryTimeout) {
    assert (timerTaskFuture == null);
    if (queryTimeout > 0) {
      timerTaskFuture =
          SchedulerProvider.getTimeoutScheduler()
              .schedule(
                  () -> {
                    try {
                      conn.cancelCurrentQuery();
                    } catch (Throwable e) {
                      // eat
                    }
                  },
                  queryTimeout,
                  TimeUnit.SECONDS);
    }
    return this;
  }

  public QueryTimeoutHandlerImpl(Connection conn, ClosableLock lock) {
    this.conn = conn;
  }

  @Override
  public void close() {
    if (timerTaskFuture != null) {
      if (!timerTaskFuture.cancel(true)) {
        // could not cancel, task either started or already finished
        // we must now wait for task to finish ensuring state modifications are done
        try {
          timerTaskFuture.get();
        } catch (InterruptedException | ExecutionException | CancellationException e) {
          // ignore error, likely due to interrupting during cancel
        }
      }
      timerTaskFuture = null;
    }
  }
}
