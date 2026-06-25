// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab

package org.mariadb.jdbc.fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.result.UpdatableResult;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.fuzz.support.FuzzColumn;
import org.mariadb.jdbc.fuzz.support.FuzzContext;
import org.mariadb.jdbc.fuzz.support.FuzzReader;

/** Professional fuzzer for MariaDB UpdatableResult state machine. */
public class MariaDbResultSetFuzzer {

  public static void fuzzerTestOneInput(FuzzedDataProvider data) {
    Reader reader = new FuzzReader(data);
    Context context = new FuzzContext(data);

    // Mock Statement to satisfy UpdatableResult constructor
    Statement stmt = null;

    try {
      int colCount = data.consumeInt(1, 10);
      ColumnDecoder[] metadata = new ColumnDecoder[colCount];
      for (int i = 0; i < colCount; i++) {
        metadata[i] = new FuzzColumn();
      }

      UpdatableResult result =
          new UpdatableResult(
              null, // Statement
              data.consumeBoolean(), // binaryProtocol
              data.consumeLong(0, 100), // maxRows
              metadata,
              reader,
              context,
              ResultSet.TYPE_SCROLL_SENSITIVE,
              data.consumeBoolean(), // closeOnCompletion
              data.consumeBoolean() // traceEnable
              );

      int iterations = data.consumeInt(1, 20);
      for (int i = 0; i < iterations; i++) {
        int action = data.consumeInt(0, 5);
        switch (action) {
          case 0:
            result.moveToInsertRow();
            break;
          case 1:
            result.insertRow();
            break;
          case 2:
            result.updateInt(data.consumeInt(1, colCount), data.consumeInt());
            break;
          case 3:
            result.updateString(data.consumeInt(1, colCount), data.consumeString(100));
            break;
          case 4:
            result.updateRow();
            break;
          case 5:
            result.deleteRow();
            break;
        }
      }
    } catch (IOException | SQLException | RuntimeException e) {
      // Expected
    }
  }
}
