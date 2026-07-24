// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;

public class PreparedStatementMetadataTest extends Common {

  @Test
  public void execute() throws SQLException {
    try (Connection con = createCon("&useServerPrepStmts=false")) {
      execute(con);
    }
    try (Connection con = createCon("&useServerPrepStmts")) {
      execute(con);
    }
  }

  private void execute(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS prepareMeta");
    stmt.execute("CREATE TABLE prepareMeta(id int, val varchar(256))");
    try (PreparedStatement prep = conn.prepareStatement("SELECT * FROM prepareMeta")) {
      ResultSetMetaData meta = prep.getMetaData();
      assertEquals(2, meta.getColumnCount());
      meta = prep.getMetaData();
      assertEquals(2, meta.getColumnCount());
    }
  }

  /**
   * A server prepare announces its column count on 2 bytes, so it cannot exceed 65535. The result
   * column count returned at execution time is length-encoded, however, so it can exceed that. This
   * checks that the maxAllowedColumns limit is enforced on the binary-protocol execute path,
   * including when metadata is resent after the underlying table changed between two executions.
   */
  @Test
  public void maxAllowedColumns() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS maxAllowedColumns");
    stmt.execute("CREATE TABLE maxAllowedColumns(a int, b int, c int)");
    stmt.execute("INSERT INTO maxAllowedColumns VALUES (1, 2, 3)");

    // execute of a 3-column result-set while limit is 2 -> rejected on execute
    try (Connection con = createCon("&useServerPrepStmts=true&maxAllowedColumns=2")) {
      try (PreparedStatement prep = con.prepareStatement("SELECT * FROM maxAllowedColumns")) {
        assertThrowsContains(
            SQLException.class,
            prep::executeQuery,
            "Server metadata announces 3 columns, exceeding the maximum allowed number of columns"
                + " (2)");
      }
    }

    // 3 columns while limit is 3 -> allowed. Then the table gains 2 columns, so the next execution
    // makes the server resend metadata (5 columns) -> rejected on the re-execute path.
    try (Connection con = createCon("&useServerPrepStmts=true&maxAllowedColumns=3")) {
      Statement s = con.createStatement();
      try (PreparedStatement prep = con.prepareStatement("SELECT * FROM maxAllowedColumns")) {
        ResultSet rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(3, rs.getMetaData().getColumnCount());

        s.execute("ALTER TABLE maxAllowedColumns ADD COLUMN d int, ADD COLUMN e int");
        assertThrowsContains(
            SQLException.class,
            prep::executeQuery,
            "Server metadata announces 5 columns, exceeding the maximum allowed number of columns"
                + " (3)");
      }
    }

    // default (65535) leaves normal result-sets unaffected
    try (Connection con = createCon("&useServerPrepStmts=true")) {
      try (PreparedStatement prep = con.prepareStatement("SELECT * FROM maxAllowedColumns")) {
        ResultSet rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(5, rs.getMetaData().getColumnCount());
      }
    }
    stmt.execute("DROP TABLE IF EXISTS maxAllowedColumns");
  }
}
