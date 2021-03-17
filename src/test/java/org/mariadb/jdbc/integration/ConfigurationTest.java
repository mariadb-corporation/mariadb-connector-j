/*
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

package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;

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
      assertEquals(5, rs.getInt(1));
      assertFalse(rs.next());
      assertFalse(stmt.getMoreResults());
      rs.clearWarnings();
    }

    try (Connection connection =
        createCon("sessionVariables=session_track_system_variables='some\\';f,ff'")) {
      Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT @@session_track_system_variables");
      assertTrue(rs.next());
      System.out.println(rs.getString(1));
    }
  }

  @Test
  public void connectionAttributes() throws SQLException {

    try (org.mariadb.jdbc.Connection conn =
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
