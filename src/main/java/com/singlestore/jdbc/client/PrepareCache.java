// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.client;

import com.singlestore.jdbc.ServerPreparedStatement;
import com.singlestore.jdbc.message.server.CachedPrepareResultPacket;
import com.singlestore.jdbc.message.server.PrepareResultPacket;
import java.util.LinkedHashMap;
import java.util.Map;

public final class PrepareCache extends LinkedHashMap<String, CachedPrepareResultPacket> {

  private static final long serialVersionUID = -8922905563713952695L;
  private final int maxSize;
  private final ClientImpl con;

  public PrepareCache(int size, ClientImpl con) {
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

  public synchronized CachedPrepareResultPacket get(
      String key, ServerPreparedStatement preparedStatement) {
    CachedPrepareResultPacket prepare = super.get(key);
    if (prepare != null && preparedStatement != null) {
      prepare.incrementUse(preparedStatement);
    }
    return prepare;
  }

  public synchronized CachedPrepareResultPacket put(
      String key, CachedPrepareResultPacket result, ServerPreparedStatement preparedStatement) {
    CachedPrepareResultPacket cached = super.get(key);

    // if there is already some cached data, return existing cached data
    if (cached != null) {
      cached.incrementUse(preparedStatement);
      result.unCache(con);
      return cached;
    }

    if (result.cache()) {
      result.incrementUse(preparedStatement);
      super.put(key, result);
    }
    return null;
  }

  public CachedPrepareResultPacket get(Object key) {
    throw new IllegalStateException("not available method");
  }

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
