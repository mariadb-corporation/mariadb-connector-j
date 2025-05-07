// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
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
  private static final Map<Configuration, PoolHolder> poolMap = new ConcurrentHashMap<>();
  private static ScheduledThreadPoolExecutor poolExecutor = null;

  static class PoolHolder {
    private final Configuration conf;
    private final int poolIndex;
    private final ScheduledThreadPoolExecutor executor;
    private Pool pool;

    PoolHolder(Configuration conf, int poolIndex, ScheduledThreadPoolExecutor executor) {
      this.conf = conf;
      this.poolIndex = poolIndex;
      this.executor = executor;
    }

    synchronized Pool getPool() {
      if (pool == null) {
        pool = new Pool(conf, poolIndex, executor);
      }
      return pool;
    }
  }

  /**
   * Get existing pool for a configuration. Create it if it doesn't exist.
   *
   * @param conf configuration parser
   * @return pool
   */
  public static Pool retrievePool(Configuration conf) {
    PoolHolder holder = poolMap.get(conf);
    if (holder == null) {
      synchronized (poolMap) {
        holder = poolMap.get(conf);
        if (holder == null) {
          if (poolExecutor == null) {
            poolExecutor =
                new ScheduledThreadPoolExecutor(
                    1, new PoolThreadFactory("MariaDbPool-maxTimeoutIdle-checker"));
          }
          holder = new PoolHolder(conf, poolIndex.incrementAndGet(), poolExecutor);
          poolMap.put(conf, holder);
        }
      }
    }
    // Don't initialize a pool while holding a lock on `poolMap`.
    return holder.getPool();
  }

  /**
   * Remove pool.
   *
   * @param pool pool to remove
   */
  public static void remove(Pool pool) {
    if (poolMap.containsKey(pool.getConf())) {
      synchronized (poolMap) {
        PoolHolder previous = poolMap.remove(pool.getConf());
        if (previous != null && poolMap.isEmpty()) {
          shutdownExecutor();
        }
      }
    }
  }

  /** Close all pools. */
  public static void close() {
    synchronized (poolMap) {
      for (PoolHolder holder : poolMap.values()) {
        try {
          holder.getPool().close();
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
      for (PoolHolder holder : poolMap.values()) {
        if (poolName.equals(holder.conf.poolName())) {
          try {
            holder
                .getPool()
                .close(); // Pool.close() calls Pools.remove(), which does the rest of the cleanup
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
