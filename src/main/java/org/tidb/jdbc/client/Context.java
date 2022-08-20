// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.client;

import org.tidb.jdbc.Configuration;
import org.tidb.jdbc.export.ExceptionFactory;

public interface Context {

  /**
   * return current thread id
   *
   * @return current server thread id
   */
  long getThreadId();

  /**
   * Get connection initial seed
   *
   * @return initial seed
   */
  byte[] getSeed();

  /**
   * has server capability
   *
   * @return true if server has capability
   */
  boolean hasServerCapability(long flag);

  /**
   * has client capability
   *
   * @return true if client has capability
   */
  boolean hasClientCapability(long flag);

  /**
   * Get server connection state
   *
   * @return server status
   */
  int getServerStatus();

  /**
   * Set server connection state
   *
   * @param serverStatus server status
   */
  void setServerStatus(int serverStatus);

  /**
   * Get current connection database
   *
   * @return database
   */
  String getDatabase();

  /**
   * set current database context
   *
   * @param database database
   */
  void setDatabase(String database);

  /**
   * Retrieve server version information
   *
   * @return server version
   */
  ServerVersion getVersion();

  /**
   * does protocol remove EOF in exchanges
   *
   * @return if EOF packet are deprecated
   */
  boolean isEofDeprecated();

  /**
   * Can server skip prepared statement metadata
   *
   * @return true if possible
   */
  boolean canSkipMeta();

  /**
   * Does server metadata exchange extended information
   *
   * @return use metadata extended information
   */
  boolean isExtendedInfo();

  /**
   * has server warnings
   *
   * @return has warnings
   */
  int getWarning();

  /**
   * set server state warning number
   *
   * @param warning warning number
   */
  void setWarning(int warning);

  /**
   * Get connection exception factory
   *
   * @return exception factory
   */
  ExceptionFactory getExceptionFactory();

  /**
   * Get connection configuration
   *
   * @return configuration
   */
  Configuration getConf();

  /**
   * Get connection transaction isolation level
   *
   * @return connection transaction isolation level
   */
  int getTransactionIsolationLevel();

  /**
   * Set current connection transaction isolation level
   *
   * @param transactionIsolationLevel new connection transaction isolation level
   */
  void setTransactionIsolationLevel(int transactionIsolationLevel);

  /**
   * get LRU prepare cache object
   *
   * @return prepare cache
   */
  PrepareCache getPrepareCache();

  /** Reset prepare cache (after a failover) */
  void resetPrepareCache();

  /**
   * return connection current state change flag
   *
   * @return connection current state change flag
   */
  int getStateFlag();

  /** reset connection state change flag */
  void resetStateFlag();

  /**
   * Indicate connection state (for pooling)
   *
   * @param state indicate that some connection state has changed
   */
  void addStateFlag(int state);
}
