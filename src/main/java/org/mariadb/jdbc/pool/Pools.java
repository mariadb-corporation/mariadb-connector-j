// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.pool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.mariadb.jdbc.Configuration;

/** Pools */
public final class Pools {

  private static final AtomicInteger poolIndex = new AtomicInteger();
  private static final Map<Configuration, Pool> poolMap = new ConcurrentHashMap<>();
  private static ScheduledThreadPoolExecutor poolExecutor = null;

  /**
   * Get existing pool for a configuration. Create it if it doesn't exist.
   *
   * @param conf configuration parser
   * @return pool
   */
  public static Pool retrievePool(Configuration conf) {
    if (!poolMap.containsKey(conf)) {
      synchronized (poolMap) {
        if (!poolMap.containsKey(conf)) {
          if (poolExecutor == null) {
            poolExecutor =
                new ScheduledThreadPoolExecutor(
                    1, new PoolThreadFactory("MariaDbPool-maxTimeoutIdle-checker"));
          }
          Pool pool = new Pool(conf, poolIndex.incrementAndGet(), poolExecutor);
          poolMap.put(conf, pool);
          return pool;
        }
      }
    }
    return poolMap.get(conf);
  }

  /**
   * Remove pool.
   *
   * @param pool pool to remove
   */
  public static void remove(Pool pool) {
    if (poolMap.containsKey(pool.getConf())) {
      synchronized (poolMap) {
        if (poolMap.containsKey(pool.getConf())) {
          poolMap.remove(pool.getConf());

          if (poolMap.isEmpty()) {
            shutdownExecutor();
          }
        }
      }
    }
  }

  /** Close all pools. */
  public static void close() {
    synchronized (poolMap) {
      for (Pool pool : poolMap.values()) {
        try {
          pool.close();
        } catch (Exception exception) {
          // eat
        }
      }
      shutdownExecutor();
      poolMap.clear();
    }
  }

  /**
   * Closing a pool with name defined in url.
   *
   * @param poolName the option "poolName" value
   */
  public static void close(String poolName) {
    if (poolName == null) {
      return;
    }
    synchronized (poolMap) {
      for (Pool pool : poolMap.values()) {
        if (poolName.equals(pool.getConf().poolName())) {
          try {
            pool.close(); // Pool.close() calls Pools.remove(), which does the rest of the cleanup
          } catch (Exception exception) {
            // eat
          }
          return;
        }
      }
    }
  }

  private static void shutdownExecutor() {
    if (poolExecutor != null) {
      poolExecutor.shutdown();
      try {
        poolExecutor.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException interrupted) {
        // eat
      }
      poolExecutor = null;
    }
  }
}
