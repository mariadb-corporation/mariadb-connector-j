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
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Common;
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
}
