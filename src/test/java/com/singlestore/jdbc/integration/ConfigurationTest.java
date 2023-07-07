// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ConfigurationTest extends Common {

  @BeforeAll
  public static void begin() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE testSessionVariable(id int not null primary key auto_increment, test varchar(10))");
    stmt.execute("FLUSH TABLES");
  }

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS testSessionVariable");
  }

  @Test
  public void testSessionVariable() throws SQLException {
    try (Connection connection =
        createCon("sessionVariables=auto_increment_increment=2&allowMultiQueries=true")) {
      Statement stmt = connection.createStatement();
      stmt.execute(
          "INSERT INTO testSessionVariable (test) values ('bb'),('cc');"
              + "INSERT INTO testSessionVariable (test) values ('dd'),('ee')",
          Statement.RETURN_GENERATED_KEYS);

      ResultSet rs = stmt.getGeneratedKeys();

      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      assertFalse(rs.next());
      assertFalse(stmt.getMoreResults());
      rs.clearWarnings();
    }

    try (Connection connection = createCon("sessionVariables=net_read_timeout=50000")) {
      Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT @@net_read_timeout");
      assertTrue(rs.next());
    }
  }

  @Test
  public void connectionAttributes() throws SQLException {

    try (com.singlestore.jdbc.Connection conn =
        createCon("&connectionAttributes=test:test1,test2:test2Val,test3")) {
      Statement stmt = conn.createStatement();
      ResultSet rs1 = stmt.executeQuery("SELECT @@performance_schema");
      rs1.next();
      if ("1".equals(rs1.getString(1))) {
        ResultSet rs =
            stmt.executeQuery(
                "SELECT * from performance_schema.session_connect_attrs where processlist_id="
                    + conn.getThreadId()
                    + " AND ATTR_NAME like 'test%'");
        assertTrue(rs.next());
        assertEquals("test1", rs.getString("ATTR_VALUE"));
        assertTrue(rs.next());
        assertEquals("test2Val", rs.getString("ATTR_VALUE"));
        assertTrue(rs.next());
        assertNull(rs.getString("ATTR_VALUE"));
      }
    }
  }
}
