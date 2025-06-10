// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import java.sql.SQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Connection;

public class GaleraTest extends Common {

  @Test
  public void galeraAllowedStatesTest() throws SQLException {
    if ("galera".equals(System.getenv("DB_TYPE"))) {
      try (Connection con = createCon("galeraAllowedState=4,5,6")) {
        con.isValid(1);
      }
    } else {
      try {
        createCon("galeraAllowedState=4,5,6");
        Assertions.fail();
      } catch (SQLException e) {
        Assertions.assertTrue(e.getMessage().contains("Initialization command fail"));
        Assertions.assertTrue(
            e.getCause()
                .getMessage()
                .contains("fail to validate Galera state (unknown 'wsrep_local_state' state)"));
      }
    }
  }
}
