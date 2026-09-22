// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.client.util.SchedulerProvider;
import org.mariadb.jdbc.pool.Pools;

/**
 * The client-side query timeout thread is driver-wide. It must be stoppable (application servers
 * unloading the driver) and must not outlive an idle driver.
 */
public class SchedulerProviderTest extends Common {

  private static Thread timeoutThread() {
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if ("MariaDb-timeout".equals(t.getName()) && t.isAlive()) return t;
    }
    return null;
  }

  private static void runWithClientTimeout(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.setQueryTimeout(10);
      try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
        assertTrue(rs.next());
      }
    }
  }

  @Test
  public void closeStopsTimeoutThreadAndUsageRecreatesIt() throws Exception {
    // canUseServerTimeout=false forces the client-side timeout path used against MySQL
    try (Connection conn = createCon("&canUseServerTimeout=false")) {
      runWithClientTimeout(conn);
      Thread thread = timeoutThread();
      assertNotNull(thread, "first query with a timeout must start the timeout thread");
      assertTrue(thread.isDaemon());

      SchedulerProvider.close();
      thread.join(TimeUnit.SECONDS.toMillis(10));
      assertFalse(thread.isAlive(), "SchedulerProvider.close() must stop the timeout thread");

      // idempotent
      SchedulerProvider.close();

      // the driver stays usable: an existing connection gets a fresh scheduler
      runWithClientTimeout(conn);
      Thread recreated = timeoutThread();
      assertNotNull(recreated, "usage after close() must recreate the scheduler");
      assertNotSame(thread, recreated);
    } finally {
      SchedulerProvider.close();
    }
  }

  @Test
  public void poolsCloseStopsTimeoutThread() throws Exception {
    try (Connection conn = createCon("&canUseServerTimeout=false")) {
      runWithClientTimeout(conn);
      Thread thread = timeoutThread();
      assertNotNull(thread);

      Pools.close();
      thread.join(TimeUnit.SECONDS.toMillis(10));
      assertFalse(thread.isAlive(), "Pools.close() must also stop the timeout thread");
    } finally {
      SchedulerProvider.close();
    }
  }

  @Test
  public void idleCoreThreadTimesOut() {
    try {
      ScheduledThreadPoolExecutor scheduler = SchedulerProvider.getTimeoutScheduler();
      assertSame(scheduler, SchedulerProvider.getTimeoutScheduler());
      assertTrue(
          scheduler.allowsCoreThreadTimeOut(),
          "the timeout thread must exit when idle so an idle driver holds no live thread");
      assertEquals(60, scheduler.getKeepAliveTime(TimeUnit.SECONDS));
      assertTrue(scheduler.getRemoveOnCancelPolicy());
    } finally {
      SchedulerProvider.close();
    }
  }
}
