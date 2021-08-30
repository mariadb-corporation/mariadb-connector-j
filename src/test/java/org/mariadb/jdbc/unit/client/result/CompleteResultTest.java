// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.unit.client.result;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.result.CompleteResult;
import org.mariadb.jdbc.integration.Common;

public class CompleteResultTest extends Common {

  /** SELECT query cannot be rewritable. */
  @Test
  public void metaQuery() throws SQLException {
    // SELECT query cannot be rewritable
    String[] columnNames =
        new String[] {
          "small",
          "big123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
        };
    DataType[] columnTypes = new DataType[] {DataType.STRING, DataType.STRING};
    String[][] data =
        new String[][] {
          new String[] {
            "small",
            "big123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
          }
        };
    ResultSet rs =
        CompleteResult.createResultSet(columnNames, columnTypes, data, sharedConn.getContext());
    assertTrue(rs.next());
    assertEquals(data[0][0], rs.getString(1));
    assertEquals(data[0][1], rs.getString(2));
  }
}
