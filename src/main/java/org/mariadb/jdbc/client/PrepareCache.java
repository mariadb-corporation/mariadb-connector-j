package org.mariadb.jdbc.client;

import java.util.LinkedHashMap;
import java.util.Map;
import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.message.server.CachedPrepareResultPacket;
import org.mariadb.jdbc.message.server.PrepareResultPacket;

public class PrepareCache extends LinkedHashMap<String, CachedPrepareResultPacket> {

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
