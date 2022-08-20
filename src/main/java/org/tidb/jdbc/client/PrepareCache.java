package org.tidb.jdbc.client;

import org.tidb.jdbc.ServerPreparedStatement;
import org.tidb.jdbc.export.Prepare;

/** LRU Prepare cache */
public interface PrepareCache {

  /**
   * Get cache value for key
   *
   * @param key key
   * @param preparedStatement prepared statement
   * @return Prepare value
   */
  Prepare get(String key, ServerPreparedStatement preparedStatement);

  /**
   * Add a prepare cache value
   *
   * @param key key
   * @param result value
   * @param preparedStatement prepared statement
   * @return Prepare if was already cached
   */
  Prepare put(String key, Prepare result, ServerPreparedStatement preparedStatement);

  /** Reset cache */
  void reset();
}
