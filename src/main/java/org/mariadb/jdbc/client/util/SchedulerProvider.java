// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.client.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public final class SchedulerProvider {
  private static ScheduledThreadPoolExecutor timeoutScheduler;

  @SuppressWarnings("try")
  public static ScheduledThreadPoolExecutor getTimeoutScheduler(ClosableLock lock) {
    if (timeoutScheduler == null) {
      try (ClosableLock ignore = lock.closeableLock()) {
        if (timeoutScheduler == null) {
          timeoutScheduler =
              new ScheduledThreadPoolExecutor(
                  1,
                  runnable -> {
                    Thread result = Executors.defaultThreadFactory().newThread(runnable);
                    result.setName("MariaDb-timeout");
                    result.setDaemon(true);
                    return result;
                  });
          timeoutScheduler.setRemoveOnCancelPolicy(true);
        }
      }
    }
    return timeoutScheduler;
  }
}
