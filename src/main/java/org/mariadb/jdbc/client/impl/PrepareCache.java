// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.impl;

import java.util.LinkedHashMap;
import java.util.Map;
import org.mariadb.jdbc.export.Prepare;
import org.mariadb.jdbc.message.server.CachedPrepareResultPacket;
import org.mariadb.jdbc.message.server.PrepareResultPacket;

/** LRU prepare cache */
public final class PrepareCache extends LinkedHashMap<String, PrepareEntry>
    implements org.mariadb.jdbc.client.PrepareCache {

  private static final long serialVersionUID = -8922905563713952695L;

  /** cache maximum size */
  private final int maxSize;

  /** client */
  private final transient StandardClient con;

  /** threshold for promoting to server-side preparation */
  private final int prepareThreshold;

  /**
   * LRU prepare cache constructor
   *
   * @param size hot cache size
   * @param con client
   * @param prepareThreshold execution count threshold for server-side promotion
   */
  public PrepareCache(int size, StandardClient con, int prepareThreshold) {
    super(size, .75f, true);
    this.maxSize = size;
    this.con = con;
    this.prepareThreshold = prepareThreshold;
  }

  @Override
  public boolean removeEldestEntry(Map.Entry<String, PrepareEntry> eldest) {
    if (this.size() > maxSize) {
      PrepareEntry entry = eldest.getValue();
      if (entry.hasServerPrepare()) {
        entry.getPrepareResult().unCache(con);
      }
      return true;
    }
    return false;
  }

  public synchronized CachedPrepareResultPacket get(String key) {
    PrepareEntry entry = super.get(key);
    return entry != null ? entry.getPrepareResult() : null;
  }

  public synchronized CachedPrepareResultPacket put(String key, Prepare result) {
    PrepareEntry entry = super.get(key);
    CachedPrepareResultPacket cached = (CachedPrepareResultPacket) result;

    // if there is already some cached data, return existing cached data
    if (entry != null && entry.hasServerPrepare()) {
      cached.unCache(con);
      return entry.getPrepareResult();
    }

    if (cached.cache()) {
      if (entry != null) {
        entry.setPrepareResult(cached);
      } else {
        entry = new PrepareEntry();
        entry.setPrepareResult(cached);
        super.put(key, entry);
      }
    }
    return null;
  }

  public PrepareEntry get(Object key) {
    throw new IllegalStateException("not available method");
  }

  /**
   * NOT USED
   *
   * @param key key
   * @param result results
   * @return will throw an exception
   */
  @SuppressWarnings("unused")
  public PrepareEntry put(String key, PrepareResultPacket result) {
    throw new IllegalStateException("not available method");
  }

  /**
   * Increment execution count for a query and check if it should be promoted to server-side.
   *
   * @param key cache key
   * @return true if query has reached the threshold and should use server-side preparation
   */
  public synchronized boolean incrementExecutionCount(String key) {
    PrepareEntry entry = super.computeIfAbsent(key, k -> new PrepareEntry());
    int newCount = entry.incrementAndGet();
    return newCount >= prepareThreshold;
  }

  /**
   * Get the current execution count for a query.
   *
   * @param key cache key
   * @return execution count, or 0 if not tracked
   */
  public synchronized int getExecutionCount(String key) {
    PrepareEntry entry = super.get(key);
    return entry != null ? entry.getExecutionCount() : 0;
  }

  public void reset() {
    for (PrepareEntry entry : values()) {
      if (entry.hasServerPrepare()) {
        entry.getPrepareResult().reset();
      }
    }
    this.clear();
  }
}
