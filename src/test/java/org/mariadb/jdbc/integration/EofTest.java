// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Connection;

public class EofTest extends Common {

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    createSequenceTables();
  }

  @Test
  public void basicResultset() throws Exception {
    basicResultset(sharedConn);
    basicResultset(sharedConnBinary);
    try (Connection con = createCon("deprecateEof=false")) {
      basicResultset(con);
    }
    try (Connection con =
        createCon("deprecateEof=false&useServerPrepStmts=true&enableSkipMeta=false")) {
      basicResultset(con);
    }
    try (Connection con =
        createCon("deprecateEof=false&useServerPrepStmts=true&enableSkipMeta=true")) {
      basicResultset(con);
    }
  }

  public void basicResultset(Connection con) throws Exception {
    try (PreparedStatement prep =
        con.prepareStatement("SELECT * FROM sequence_1_to_10 where 1 = ?")) {
      prep.setFetchSize(2);
      prep.setMaxRows(4);
      prep.setInt(1, 1);
      ResultSet rs = prep.executeQuery();
      rs.next();
      assertEquals(1, rs.getInt(1));
    }
  }
}
