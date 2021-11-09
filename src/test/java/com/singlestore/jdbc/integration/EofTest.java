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
import org.junit.jupiter.api.Test;

public class EofTest extends Common {

  @Test
  public void basicResultset() throws Exception {
    try (Connection con = createCon("useEof=false")) {
      Statement stmt = con.createStatement();
      ensureRange(stmt);
      basicResultset(con);
    }
    try (Connection con = createCon("useEof=true&useServerPrepStmts=true")) {
      Statement stmt = con.createStatement();
      ensureRange(stmt);
      basicResultset(con);
    }
  }

  public void basicResultset(Connection con) throws Exception {
    try (PreparedStatement prep =
        con.prepareStatement("SELECT * FROM range_1_100  where n = ? ORDER BY n LIMIT 10")) {
      prep.setFetchSize(2);
      prep.setMaxRows(4);
      prep.setInt(1, 1);
      ResultSet rs = prep.executeQuery();
      rs.next();
      assertEquals(1, rs.getInt(1));
    }
  }
}
