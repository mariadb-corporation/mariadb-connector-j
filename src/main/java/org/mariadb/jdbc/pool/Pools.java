/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.pool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.mariadb.jdbc.Configuration;

public class Pools {

  private static final AtomicInteger poolIndex = new AtomicInteger();
  private static final Map<Configuration, Pool> poolMap = new ConcurrentHashMap<>();
  private static ScheduledThreadPoolExecutor poolExecutor = null;

  /**
   * Get existing pool for a configuration. Create it if doesn't exists.
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
    poolExecutor.shutdown();
    try {
      poolExecutor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException interrupted) {
      // eat
    }
    poolExecutor = null;
  }
}
