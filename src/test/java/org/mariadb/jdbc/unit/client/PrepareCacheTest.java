// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.impl.PrepareCache;

public class PrepareCacheTest {

  @Test
  public void testPrepareThresholdBasics() {
    PrepareCache cache = new PrepareCache(3, null, 3);

    // Test execution count tracking
    assertEquals(0, cache.getExecutionCount("query1"));
    
    // First execution - should not reach threshold
    assertFalse(cache.incrementExecutionCount("query1"));
    assertEquals(1, cache.getExecutionCount("query1"));
    
    // Second execution
    assertFalse(cache.incrementExecutionCount("query1"));
    assertEquals(2, cache.getExecutionCount("query1"));
    
    // Third execution - reaches threshold (3)
    assertTrue(cache.incrementExecutionCount("query1"));
    assertEquals(3, cache.getExecutionCount("query1"));
    
    // Non-existent key should return 0
    assertEquals(0, cache.getExecutionCount("nonexistent"));
  }

  @Test
  public void testCacheLRUEviction() {
    PrepareCache cache = new PrepareCache(2, null, 5); // Cache size = 2

    // Add 3 items to cache (should evict first one)
    cache.incrementExecutionCount("query1");
    cache.incrementExecutionCount("query2");
    cache.incrementExecutionCount("query3"); // This should evict query1

    assertEquals(0, cache.getExecutionCount("query1"), "query1 should be evicted");
    assertEquals(1, cache.getExecutionCount("query2"), "query2 should still be in cache");
    assertEquals(1, cache.getExecutionCount("query3"), "query3 should be in cache");
  }

  @Test
  public void testCacheInsertionOrder() {
    PrepareCache cache = new PrepareCache(3, null, 5); // Cache size = 3

    // Add three items - fills the cache
    cache.incrementExecutionCount("query1");
    cache.incrementExecutionCount("query2");
    cache.incrementExecutionCount("query3");

    // All three should be in cache
    assertEquals(1, cache.getExecutionCount("query1"));
    assertEquals(1, cache.getExecutionCount("query2"));
    assertEquals(1, cache.getExecutionCount("query3"));

    // Add query4 - should evict query1 (oldest insertion)
    cache.incrementExecutionCount("query4");

    assertEquals(0, cache.getExecutionCount("query1"), "query1 should be evicted (oldest)");
    assertEquals(1, cache.getExecutionCount("query2"), "query2 should remain");
    assertEquals(1, cache.getExecutionCount("query3"), "query3 should remain");
    assertEquals(1, cache.getExecutionCount("query4"), "query4 should be in cache");
  }

  @Test
  public void testThresholdPromotionFlow() {
    PrepareCache cache = new PrepareCache(10, null, 3);

    // Simulate the flow: query executed multiple times until threshold
    String query = "SELECT * FROM users WHERE id = ?";

    // First execution - count = 1, not promoted
    assertFalse(cache.incrementExecutionCount(query));
    assertEquals(1, cache.getExecutionCount(query));

    // Second execution - count = 2, not promoted
    assertFalse(cache.incrementExecutionCount(query));
    assertEquals(2, cache.getExecutionCount(query));

    // Third execution - count = 3, reaches threshold, should promote
    assertTrue(cache.incrementExecutionCount(query), "Query should reach threshold for promotion");
    assertEquals(3, cache.getExecutionCount(query));
  }

  @Test
  public void testCacheReset() {
    PrepareCache cache = new PrepareCache(10, null, 3);

    // Add execution counts
    cache.incrementExecutionCount("query1");
    cache.incrementExecutionCount("query2");

    assertEquals(1, cache.getExecutionCount("query1"));
    assertEquals(1, cache.getExecutionCount("query2"));

    // Reset should clear cache
    cache.reset();

    assertEquals(0, cache.getExecutionCount("query1"), "Cache should be cleared");
    assertEquals(0, cache.getExecutionCount("query2"), "Cache should be cleared");
  }

  @Test
  public void testEmptyCache() {
    PrepareCache cache = new PrepareCache(10, null, 3);

    assertEquals(0, cache.getExecutionCount("anything"));
    assertNull(cache.get("anything"));
  }

  @Test
  public void testZeroThreshold() {
    PrepareCache cache = new PrepareCache(10, null, 0); // Threshold = 0, immediate promotion

    // With threshold 0, first increment should reach threshold
    assertTrue(cache.incrementExecutionCount("query1"));
    assertEquals(1, cache.getExecutionCount("query1"));
  }

  @Test
  public void testMultipleCacheOperations() {
    PrepareCache cache = new PrepareCache(5, null, 3); // Cache size = 5

    // Add multiple queries
    for (int i = 1; i <= 5; i++) {
      cache.incrementExecutionCount("query" + i);
    }

    // All should be in cache with count = 1
    for (int i = 1; i <= 5; i++) {
      assertEquals(1, cache.getExecutionCount("query" + i), "query" + i + " should be in cache");
    }

    // Add one more - should evict query1
    cache.incrementExecutionCount("query6");
    assertEquals(0, cache.getExecutionCount("query1"), "query1 should be evicted");
    assertEquals(1, cache.getExecutionCount("query6"), "query6 should be in cache");

    // Increment some queries multiple times
    cache.incrementExecutionCount("query2");
    cache.incrementExecutionCount("query2");
    
    assertEquals(3, cache.getExecutionCount("query2"));
    assertEquals(1, cache.getExecutionCount("query3"));
    assertEquals(1, cache.getExecutionCount("query4"));
  }

  @Test
  public void testCacheGet() {
    PrepareCache cache = new PrepareCache(10, null, 3);

    // Cache should return null for non-existent keys
    assertNull(cache.get("nonexistent"));
    
    // Cache should return null for keys with only execution count (no server prepare)
    cache.incrementExecutionCount("query1");
    assertNull(cache.get("query1"), "Should return null when no server-side prepare exists");
  }
}
