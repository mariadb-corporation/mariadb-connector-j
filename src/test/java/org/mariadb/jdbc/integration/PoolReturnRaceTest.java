// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.pool.MariaDbInnerPoolConnection;
import org.mariadb.jdbc.pool.Pool;

public class PoolReturnRaceTest extends Common {
  @Test
  @Timeout(20)
  public void reattachBeforePublishingReturnedConnection() throws Exception {
    ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
    ExecutorService returner = Executors.newSingleThreadExecutor();
    CountDownLatch published = new CountDownLatch(1);
    CountDownLatch borrowedAndClosed = new CountDownLatch(1);
    AtomicBoolean intercept = new AtomicBoolean(true);
    try (Pool pool =
        new Pool(Configuration.parse(mDefUrl + "&maxPoolSize=1&minPoolSize=1"), 0, scheduler)) {
      Connection original = pool.getPoolConnection().getConnection();
      LinkedBlockingDeque<MariaDbInnerPoolConnection> queue =
          new LinkedBlockingDeque<MariaDbInnerPoolConnection>() {
            @Override
            public void addFirst(MariaDbInnerPoolConnection item) {
              super.addFirst(item);
              if (intercept.compareAndSet(true, false)) {
                published.countDown();
                try {
                  if (!borrowedAndClosed.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("Borrower did not finish");
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new AssertionError(e);
                }
              }
            }
          };
      // Pause the return immediately after publication, allowing a real second borrower to close
      // it.
      Field idleConnections = Pool.class.getDeclaredField("idleConnections");
      idleConnections.setAccessible(true);
      idleConnections.set(pool, queue);
      Future<?> returning =
          returner.submit(
              () -> {
                original.close();
                return null;
              });
      try {
        assertTrue(published.await(5, TimeUnit.SECONDS));
        Connection borrowed = pool.getPoolConnection().getConnection();
        assertSame(original, borrowed);
        borrowed.close();
      } finally {
        borrowedAndClosed.countDown();
      }
      returning.get(5, TimeUnit.SECONDS);
      assertFalse(original.isClosed(), "Returning a pooled connection must not close its socket");
      assertEquals(1, pool.getTotalConnections());
      assertEquals(1, pool.getIdleConnections());
      assertEquals(0, pool.getActiveConnections());
      try (Connection usable = pool.getPoolConnection().getConnection();
          java.sql.Statement statement = usable.createStatement();
          java.sql.ResultSet result = statement.executeQuery("SELECT 1")) {
        assertTrue(result.next());
        assertEquals(1, result.getInt(1));
      }
    } finally {
      borrowedAndClosed.countDown();
      returner.shutdownNow();
      scheduler.shutdownNow();
    }
  }
}
