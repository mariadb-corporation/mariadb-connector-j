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

package org.mariadb.jdbc.internal.util.pool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.util.scheduler.MariaDbThreadFactory;

public class Pools {

  private static final AtomicInteger poolIndex = new AtomicInteger();
  private static final Map<UrlParser, Pool> poolMap = new ConcurrentHashMap<UrlParser, Pool>();
  private static ScheduledThreadPoolExecutor poolExecutor = null;

  /**
   * Get existing pool for a configuration. Create it if doesn't exists.
   *
   * @param urlParser configuration parser
   * @return pool
   */
  public static Pool retrievePool(UrlParser urlParser) {
    if (!poolMap.containsKey(urlParser)) {
      synchronized (poolMap) {
        if (!poolMap.containsKey(urlParser)) {
          if (poolExecutor == null) {
            poolExecutor =
                new ScheduledThreadPoolExecutor(
                    1, new MariaDbThreadFactory("MariaDbPool-maxTimeoutIdle-checker"));
          }
          Pool pool = new Pool(urlParser, poolIndex.incrementAndGet(), poolExecutor);
          poolMap.put(urlParser, pool);
          return pool;
        }
      }
    }
    return poolMap.get(urlParser);
  }

  /**
   * Remove pool.
   *
   * @param pool pool to remove
   */
  public static void remove(Pool pool) {
    if (poolMap.containsKey(pool.getUrlParser())) {
      synchronized (poolMap) {
        if (poolMap.containsKey(pool.getUrlParser())) {
          poolMap.remove(pool.getUrlParser());

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
        pool.close();
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
        if (poolName.equals(pool.getUrlParser().getOptions().poolName)) {
          pool.close(); // Pool.close() calls Pools.remove(), which does the rest of the cleanup
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
