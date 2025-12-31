// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.impl;

import java.util.concurrent.atomic.AtomicInteger;

import org.mariadb.jdbc.message.server.CachedPrepareResultPacket;

/**
 * Entry in the prepare cache that tracks execution count and optionally holds
 * a server-side prepared statement result.
 */
final class PrepareEntry {
  
  /** Number of times this query has been executed */
  private final AtomicInteger executionCount;
  
  /** Server-side prepared statement result (null if not yet promoted) */
  private volatile CachedPrepareResultPacket prepareResult;
  
  /**
   * Create a new prepare entry with execution count starting at 0
   */
  PrepareEntry() {
    this.executionCount = new AtomicInteger(0);
    this.prepareResult = null;
  }
  
  /**
   * Increment execution count and return the new value
   * 
   * @return new execution count
   */
  int incrementAndGet() {
    return executionCount.incrementAndGet();
  }
  
  /**
   * Get current execution count
   * 
   * @return execution count
   */
  int getExecutionCount() {
    return executionCount.get();
  }
  
  /**
   * Set the server-side prepared statement result
   * 
   * @param prepareResult prepared statement result
   */
  void setPrepareResult(CachedPrepareResultPacket prepareResult) {
    this.prepareResult = prepareResult;
  }
  
  /**
   * Get the server-side prepared statement result
   * 
   * @return prepared statement result, or null if not yet promoted
   */
  CachedPrepareResultPacket getPrepareResult() {
    return prepareResult;
  }
  
  /**
   * Check if this entry has a server-side prepared statement
   * 
   * @return true if prepared on server
   */
  boolean hasServerPrepare() {
    return prepareResult != null;
  }
}
