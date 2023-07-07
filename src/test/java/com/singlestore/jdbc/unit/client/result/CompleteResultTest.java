// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.unit.client.result;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.result.CompleteResult;
import com.singlestore.jdbc.integration.Common;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

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
    DataType[] columnTypes = new DataType[] {DataType.CHAR, DataType.CHAR};
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
