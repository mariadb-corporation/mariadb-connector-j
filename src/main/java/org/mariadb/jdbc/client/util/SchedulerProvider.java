// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.client.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class SchedulerProvider {

  static final long IDLE_THREAD_TIMEOUT_SECONDS = 60;

  private static final Object LOCK = new Object();
  private static volatile ScheduledThreadPoolExecutor timeoutScheduler;

  private SchedulerProvider() {}

  public static ScheduledThreadPoolExecutor getTimeoutScheduler() {
    ScheduledThreadPoolExecutor scheduler = timeoutScheduler;
    if (scheduler == null) {
      synchronized (LOCK) {
        scheduler = timeoutScheduler;
        if (scheduler == null) {
          scheduler =
              new ScheduledThreadPoolExecutor(
                  1,
                  runnable -> {
                    Thread result = Executors.defaultThreadFactory().newThread(runnable);
                    result.setName("MariaDb-timeout");
                    result.setDaemon(true);
                    return result;
                  });
          scheduler.setRemoveOnCancelPolicy(true);
          scheduler.setKeepAliveTime(IDLE_THREAD_TIMEOUT_SECONDS, TimeUnit.SECONDS);
          scheduler.allowCoreThreadTimeOut(true);
          timeoutScheduler = scheduler;
        }
      }
    }
    return scheduler;
  }

  /**
   * Get the shared timeout scheduler.
   *
   * @param lock ignored, kept for source compatibility
   * @return the shared timeout scheduler
   * @deprecated use {@link #getTimeoutScheduler()}; the scheduler is guarded by its own lock
   */
  @Deprecated
  public static ScheduledThreadPoolExecutor getTimeoutScheduler(ClosableLock lock) {
    return getTimeoutScheduler();
  }

  /**
   * Shut down the shared timeout scheduler, if any, cancelling pending timeout tasks. Intended for
   * application shutdown or undeploy, after connections are closed. Safe to call several times; a
   * later use of the driver recreates the scheduler.
   */
  public static void close() {
    synchronized (LOCK) {
      ScheduledThreadPoolExecutor scheduler = timeoutScheduler;
      if (scheduler != null) {
        timeoutScheduler = null;
        scheduler.shutdownNow();
      }
    }
  }
}
