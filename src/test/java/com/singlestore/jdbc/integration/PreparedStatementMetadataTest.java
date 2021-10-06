// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Common;
import com.singlestore.jdbc.Connection;
import com.singlestore.jdbc.Statement;
import java.sql.*;
import org.junit.jupiter.api.*;

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
