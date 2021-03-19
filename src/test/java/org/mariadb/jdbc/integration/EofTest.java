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

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Connection;

public class EofTest extends Common {

  @Test
  public void basicResultset() throws Exception {
    Assumptions.assumeTrue(isMariaDBServer());
    try (Connection con = createCon("useEof=false")) {
      basicResultset(con);
    }
    try (Connection con = createCon("useEof=true&useServerPrepStmts=true")) {
      basicResultset(con);
    }
  }

  public void basicResultset(Connection con) throws Exception {
    try (PreparedStatement prep = con.prepareStatement("SELECT * FROM seq_1_to_10 where 1 = ?")) {
      prep.setFetchSize(2);
      prep.setMaxRows(4);
      prep.setInt(1, 1);
      ResultSet rs = prep.executeQuery();
      rs.next();
      assertEquals(1, rs.getInt(1));
    }
  }
}
