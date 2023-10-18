// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.client.impl;

import java.util.LinkedHashMap;
import java.util.Map;
import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.export.Prepare;
import org.mariadb.jdbc.message.server.CachedPrepareResultPacket;
import org.mariadb.jdbc.message.server.PrepareResultPacket;

/** LRU prepare cache */
public final class PrepareCache extends LinkedHashMap<String, CachedPrepareResultPacket>
    implements org.mariadb.jdbc.client.PrepareCache {

  private static final long serialVersionUID = -8922905563713952695L;

  /** cache maximum size */
  private final int maxSize;

  /** client */
  private final transient StandardClient con;

  /**
   * LRU prepare cache constructor
   *
   * @param size cache size
   * @param con client
   */
  public PrepareCache(int size, StandardClient con) {
    super(size, .75f, true);
    this.maxSize = size;
    this.con = con;
  }

  @Override
  public boolean removeEldestEntry(Map.Entry<String, CachedPrepareResultPacket> eldest) {
    if (this.size() > maxSize) {
      eldest.getValue().unCache(con);
      return true;
    }
    return false;
  }

  public synchronized Prepare get(String key, ServerPreparedStatement preparedStatement) {
    CachedPrepareResultPacket prepare = super.get(key);
    if (prepare != null && preparedStatement != null) {
      prepare.incrementUse(preparedStatement);
    }
    return prepare;
  }

  public synchronized Prepare put(
      String key, Prepare result, ServerPreparedStatement preparedStatement) {
    CachedPrepareResultPacket cached = super.get(key);

    // if there is already some cached data, return existing cached data
    if (cached != null) {
      cached.incrementUse(preparedStatement);
      ((CachedPrepareResultPacket) result).unCache(con);
      return cached;
    }

    if (((CachedPrepareResultPacket) result).cache()) {
      ((CachedPrepareResultPacket) result).incrementUse(preparedStatement);
      super.put(key, (CachedPrepareResultPacket) result);
    }
    return null;
  }

  public CachedPrepareResultPacket get(Object key) {
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
  public CachedPrepareResultPacket put(String key, PrepareResultPacket result) {
    throw new IllegalStateException("not available method");
  }

  public void reset() {
    for (CachedPrepareResultPacket prep : values()) {
      prep.reset();
    }
    this.clear();
  }
}
