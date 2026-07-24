/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A server prepare announces its column count on 2 bytes, so it cannot exceed 65535. The result
 * column count returned at execution time is length-encoded, however, so it can exceed that. The
 * maxAllowedColumns option bounds it to protect against a malicious proxy announcing a huge count
 * to exhaust client memory. The limit is enforced on the (binary and text) execute path in {@code
 * AbstractQueryProtocol.readResultSet}, including when metadata is resent after a table change.
 */
public class MaxAllowedColumnsTest extends BaseTest {

  @BeforeClass()
  public static void initClass() throws SQLException {
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS maxAllowedColumnsWide");
      stmt.execute("DROP TABLE IF EXISTS maxAllowedColumnsGrow");
      stmt.execute("CREATE TABLE maxAllowedColumnsWide(" + cols(25) + ")");
      stmt.execute("CREATE TABLE maxAllowedColumnsGrow(" + cols(15) + ")");
    }
  }

  private static String cols(int n) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n; i++) {
      if (i > 0) sb.append(",");
      sb.append("c").append(i).append(" int");
    }
    return sb.toString();
  }

  @Test
  public void maxAllowedColumnsRejectedOnExecute() throws SQLException {
    // 25-column result-set while limit is 20 -> rejected on execute
    try (Connection con = setConnection("&useServerPrepStmts=true&maxAllowedColumns=20")) {
      try (PreparedStatement prep = con.prepareStatement("SELECT * FROM maxAllowedColumnsWide")) {
        prep.executeQuery();
        fail("expected an exception, 25 columns exceed the limit of 20");
      } catch (SQLException e) {
        assertTrue(
            e.getMessage(),
            e.getMessage()
                .contains(
                    "Server metadata announces 25 columns, exceeding the maximum allowed number of"
                        + " columns (20)"));
      }
    }
  }

  @Test
  public void maxAllowedColumnsRejectedAfterTableChange() throws SQLException {
    // 15 columns while limit is 20 -> allowed. Then the table gains 10 columns, so the next
    // execution makes the server resend metadata (25 columns) -> rejected on the re-execute path.
    try (Connection con = setConnection("&useServerPrepStmts=true&maxAllowedColumns=20")) {
      Statement stmt = con.createStatement();
      try (PreparedStatement prep = con.prepareStatement("SELECT * FROM maxAllowedColumnsGrow")) {
        ResultSet rs = prep.executeQuery();
        assertEquals(15, rs.getMetaData().getColumnCount());

        StringBuilder add = new StringBuilder();
        for (int i = 15; i < 25; i++) {
          if (i > 15) add.append(",");
          add.append("ADD COLUMN c").append(i).append(" int");
        }
        stmt.execute("ALTER TABLE maxAllowedColumnsGrow " + add);

        try {
          prep.executeQuery();
          fail("expected an exception, table now has 25 columns, above the limit of 20");
        } catch (SQLException e) {
          assertTrue(
              e.getMessage(),
              e.getMessage()
                  .contains(
                      "Server metadata announces 25 columns, exceeding the maximum allowed number"
                          + " of columns (20)"));
        }
      }
    }
  }

  @Test
  public void maxAllowedColumnsDefaultAllowsWideResultSet() throws SQLException {
    // default (65535) leaves a normal wide result-set unaffected
    try (Connection con = setConnection("&useServerPrepStmts=true")) {
      try (PreparedStatement prep = con.prepareStatement("SELECT * FROM maxAllowedColumnsWide")) {
        ResultSet rs = prep.executeQuery();
        assertEquals(25, rs.getMetaData().getColumnCount());
      }
    }
  }
}
