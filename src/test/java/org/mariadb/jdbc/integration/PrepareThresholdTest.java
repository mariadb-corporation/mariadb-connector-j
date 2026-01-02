// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;

public class PrepareThresholdTest extends Common {

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS prepareThresholdTest");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE prepareThresholdTest (id int not null primary key auto_increment, value varchar(100))");
    stmt.execute("INSERT INTO prepareThresholdTest (value) VALUES ('test1'), ('test2'), ('test3')");
  }

  @Test
  public void testPrepareThresholdDefault() throws SQLException {
    // Test with default threshold (5)
    try (Connection con =
        createCon("&useServerPrepStmts=true&cachePrepStmts=true&prepStmtCacheSize=10")) {
      String sql = "SELECT * FROM prepareThresholdTest WHERE id = ?";

      // Execute 4 times - should use client-side preparation
      for (int i = 1; i <= 4; i++) {
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
          assertTrue(
              pstmt.getClass().getName().contains("ClientPreparedStatement"),
              "Execution " + i + " should use client-side preparation");
          pstmt.setInt(1, 1);
          try (ResultSet rs = pstmt.executeQuery()) {
            assertTrue(rs.next());
            assertEquals("test1", rs.getString("value"));
          }
        }
      }

      // 5th execution - should promote to server-side preparation
      try (PreparedStatement pstmt = con.prepareStatement(sql)) {
        assertTrue(
            pstmt.getClass().getName().contains("ServerPreparedStatement"),
            "5th execution should use server-side preparation");
        pstmt.setInt(1, 2);
        try (ResultSet rs = pstmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("test2", rs.getString("value"));
        }
      }

      // Subsequent executions should continue using server-side
      try (PreparedStatement pstmt = con.prepareStatement(sql)) {
        assertTrue(
            pstmt.getClass().getName().contains("ServerPreparedStatement"),
            "6th execution should use server-side preparation");
        pstmt.setInt(1, 3);
        try (ResultSet rs = pstmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("test3", rs.getString("value"));
        }
      }
    }
  }

  @Test
  public void testPrepareThresholdCustom() throws SQLException {
    // Test with custom threshold (3)
    try (Connection con =
        createCon(
            "&useServerPrepStmts=true&cachePrepStmts=true&prepStmtCacheSize=10&prepareThreshold=3")) {
      String sql = "SELECT * FROM prepareThresholdTest WHERE id = ?";

      // Execute 2 times - should use client-side preparation
      for (int i = 1; i <= 2; i++) {
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
          assertTrue(
              pstmt.getClass().getName().contains("ClientPreparedStatement"),
              "Execution " + i + " should use client-side preparation");
          pstmt.setInt(1, 1);
          try (ResultSet rs = pstmt.executeQuery()) {
            assertTrue(rs.next());
          }
        }
      }

      // 3rd execution - should promote to server-side preparation
      try (PreparedStatement pstmt = con.prepareStatement(sql)) {
        assertTrue(
            pstmt.getClass().getName().contains("ServerPreparedStatement"),
            "3rd execution should use server-side preparation");
        pstmt.setInt(1, 2);
        try (ResultSet rs = pstmt.executeQuery()) {
          assertTrue(rs.next());
        }
      }
    }
  }

  @Test
  public void testPrepareThresholdZero() throws SQLException {
    // Test with threshold 0 (immediate server-side preparation)
    try (Connection con =
        createCon(
            "&useServerPrepStmts=true&cachePrepStmts=true&prepStmtCacheSize=10&prepareThreshold=0")) {
      String sql = "SELECT * FROM prepareThresholdTest WHERE id = ?";

      // First execution should already use server-side preparation
      try (PreparedStatement pstmt = con.prepareStatement(sql)) {
        assertTrue(
            pstmt.getClass().getName().contains("ServerPreparedStatement"),
            "With threshold=0, first execution should use server-side preparation");
        pstmt.setInt(1, 1);
        try (ResultSet rs = pstmt.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("test1", rs.getString("value"));
        }
      }
    }
  }

  @Test
  public void testPrepareThresholdWithoutCache() throws SQLException {
    // Test that prepareThreshold has no effect when cachePrepStmts=false
    try (Connection con =
        createCon(
            "&useServerPrepStmts=true&cachePrepStmts=false&prepareThreshold=5")) {
      String sql = "SELECT * FROM prepareThresholdTest WHERE id = ?";

      // Should always use server-side when useServerPrepStmts=true and cachePrepStmts=false
      try (PreparedStatement pstmt = con.prepareStatement(sql)) {
        assertTrue(
            pstmt.getClass().getName().contains("ServerPreparedStatement"),
            "Without caching, should always use server-side preparation");
        pstmt.setInt(1, 1);
        try (ResultSet rs = pstmt.executeQuery()) {
          assertTrue(rs.next());
        }
      }
    }
  }

  @Test
  public void testPrepareThresholdMultipleQueries() throws SQLException {
    // Test that different queries have independent execution counts
    try (Connection con =
        createCon(
            "&useServerPrepStmts=true&cachePrepStmts=true&prepStmtCacheSize=10&prepareThreshold=3")) {
      String sql1 = "SELECT * FROM prepareThresholdTest WHERE id = ?";
      String sql2 = "SELECT * FROM prepareThresholdTest WHERE value = ?";

      // Execute sql1 twice
      for (int i = 1; i <= 2; i++) {
        try (PreparedStatement pstmt = con.prepareStatement(sql1)) {
          assertTrue(pstmt.getClass().getName().contains("ClientPreparedStatement"));
          pstmt.setInt(1, 1);
          pstmt.executeQuery().close();
        }
      }

      // Execute sql2 twice - should also be client-side (independent count)
      try (PreparedStatement pstmt = con.prepareStatement(sql2)) {
        assertTrue(
            pstmt.getClass().getName().contains("ClientPreparedStatement"),
            "sql2 execution 1 should use client-side (independent count)");
        pstmt.setString(1, "test1");
        pstmt.executeQuery().close();
      }

      // 3rd execution of sql1 - should promote to server-side
      try (PreparedStatement pstmt = con.prepareStatement(sql1)) {
        assertTrue(
            pstmt.getClass().getName().contains("ServerPreparedStatement"),
            "sql1 3rd execution should promote to server-side");
        pstmt.setInt(1, 1);
        pstmt.executeQuery().close();
      }

      // sql2 still needs one more execution to reach threshold
      try (PreparedStatement pstmt = con.prepareStatement(sql2)) {
        assertTrue(
            pstmt.getClass().getName().contains("ClientPreparedStatement"),
            "sql2 2nd execution should still be client-side");
        pstmt.setString(1, "test1");
        pstmt.executeQuery().close();
      }

      // Now sql2 should be promoted
      try (PreparedStatement pstmt = con.prepareStatement(sql2)) {
        assertTrue(
            pstmt.getClass().getName().contains("ServerPreparedStatement"),
            "sql2 3th execution should promote to server-side");
        pstmt.setString(1, "test1");
        pstmt.executeQuery().close();
      }
    }
  }

  @Test
  public void testPrepareThresholdCacheEviction() throws SQLException {
    // Test that execution counts are evicted from cache when full
    try (Connection con =
        createCon(
            "&useServerPrepStmts=true&cachePrepStmts=true&prepStmtCacheSize=2&prepareThreshold=5")) {
      
      // Execute 3 different queries once each
      // Cache size is 2, so the first query should be evicted
      String sql1 = "SELECT * FROM prepareThresholdTest WHERE id = 1";
      String sql2 = "SELECT * FROM prepareThresholdTest WHERE id = 2";
      String sql3 = "SELECT * FROM prepareThresholdTest WHERE id = 3";

      try (PreparedStatement pstmt = con.prepareStatement(sql1)) {
        pstmt.executeQuery().close();
      }
      try (PreparedStatement pstmt = con.prepareStatement(sql2)) {
        pstmt.executeQuery().close();
      }
      try (PreparedStatement pstmt = con.prepareStatement(sql3)) {
        pstmt.executeQuery().close();
      }

      // sql1 should have been evicted, so its count should reset
      // Execute sql1 again - should be treated as first execution
      try (PreparedStatement pstmt = con.prepareStatement(sql1)) {
        assertTrue(
            pstmt.getClass().getName().contains("ClientPreparedStatement"),
            "After eviction, sql1 should restart from client-side");
        pstmt.executeQuery().close();
      }
    }
  }

  @Test
  public void testPrepareThresholdWithBatch() throws SQLException {
    // Test that batch executions count toward threshold
    try (Connection con =
        createCon(
            "&useServerPrepStmts=true&cachePrepStmts=true&prepStmtCacheSize=10&prepareThreshold=3")) {
      String sql = "INSERT INTO prepareThresholdTest (value) VALUES (?)";

      // Execute batch 2 times
      for (int i = 1; i <= 2; i++) {
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
          assertTrue(
              pstmt.getClass().getName().contains("ClientPreparedStatement"),
              "Batch execution " + i + " should use client-side");
          pstmt.setString(1, "batch" + i);
          pstmt.addBatch();
          pstmt.executeBatch();
        }
      }

      // 3rd execution - should promote to server-side
      try (PreparedStatement pstmt = con.prepareStatement(sql)) {
        assertTrue(
            pstmt.getClass().getName().contains("ServerPreparedStatement"),
            "3rd batch execution should promote to server-side");
        pstmt.setString(1, "batch3");
        pstmt.addBatch();
        pstmt.executeBatch();
      }

      // Clean up inserted rows
      try (Statement stmt = con.createStatement()) {
        stmt.execute("DELETE FROM prepareThresholdTest WHERE value LIKE 'batch%'");
      }
    }
  }

  @Test
  public void testPrepareThresholdHighValue() throws SQLException {
    // Test with a high threshold value
    try (Connection con =
        createCon(
            "&useServerPrepStmts=true&cachePrepStmts=true&prepStmtCacheSize=10&prepareThreshold=100")) {
      String sql = "SELECT * FROM prepareThresholdTest WHERE id = ?";

      // Execute 10 times - should all use client-side
      for (int i = 1; i <= 10; i++) {
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
          assertTrue(
              pstmt.getClass().getName().contains("ClientPreparedStatement"),
              "With high threshold, execution " + i + " should use client-side");
          pstmt.setInt(1, 1);
          pstmt.executeQuery().close();
        }
      }
    }
  }
}
