// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab

package org.mariadb.jdbc.fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.client.util.Parameter;
import org.mariadb.jdbc.fuzz.support.*;
import org.mariadb.jdbc.message.client.*;
import org.mariadb.jdbc.plugin.Credential;

/** Fuzzer for MariaDB Protocol Messages. */
public class MariaDbMessageFuzzer {
  public static void fuzzerTestOneInput(FuzzedDataProvider data) {
    try {
      FuzzContext context = new FuzzContext();
      FuzzWriter writer = new FuzzWriter();

      int subTask = data.consumeInt(0, 4);
      switch (subTask) {
        case 0: // HandshakeResponse
          Credential credential = new Credential(data.consumeString(20), data.consumeString(20));
          HandshakeResponse response = new HandshakeResponse(
              credential, "mysql_native_password", new byte[20], context.getConf(), "localhost", 0, (byte) 33);
          response.encode(writer, context);
          break;

        case 1: // QueryPacket
          QueryPacket query = new QueryPacket(data.consumeString(100));
          query.encode(writer, context);
          break;

        case 2: // ExecutePacket
          int paramCount = data.consumeInt(0, 5);
          FuzzPrepare prepare = new FuzzPrepare(paramCount);
          Parameter[] params = new Parameter[paramCount];
          for (int i = 0; i < params.length; i++) {
            params[i] = new FuzzParameter(FuzzValueGenerator.pickCodec(data), FuzzValueGenerator.generateValue(data));
          }
          ExecutePacket execute = new ExecutePacket(prepare, new FuzzParameters(params), data.consumeString(100), null, null);
          execute.encode(writer, context, prepare);
          break;

        case 3: // PreparePacket
          PreparePacket prep = new PreparePacket(data.consumeString(100));
          prep.encode(writer, context);
          break;

        case 4: // QuitPacket
          QuitPacket quit = QuitPacket.INSTANCE;
          quit.encode(writer, context);
          break;
      }
    } catch (IOException | SQLException | RuntimeException e) {
      // Expected
    }
  }
}
