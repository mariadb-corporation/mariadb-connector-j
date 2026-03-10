// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab

package org.mariadb.jdbc.fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.fuzz.support.FuzzContext;
import org.mariadb.jdbc.fuzz.support.FuzzReader;
import org.mariadb.jdbc.fuzz.support.FuzzWriter;
import org.mariadb.jdbc.message.client.HandshakeResponse;
import org.mariadb.jdbc.plugin.Credential;
import org.mariadb.jdbc.plugin.authentication.addon.ClearPasswordPlugin;
import org.mariadb.jdbc.plugin.authentication.standard.CachingSha2PasswordPlugin;
import org.mariadb.jdbc.plugin.authentication.standard.NativePasswordPlugin;

/** Professional fuzzer for MariaDB client-side logic (Auth, Handshake, Config). */
public class MariaDbClientLogicFuzzer {
  private static final Configuration conf;

  static {
    Configuration c = null;
    try {
      c = Configuration.parse("jdbc:mariadb://localhost/test?user=test&password=test");
    } catch (SQLException e) {
      // Ignored
    }
    conf = c;
  }

  private static final Credential credential = new Credential("test", "test");
  private static final HostAddress hostAddress = HostAddress.from("localhost", 3306);

  public static void fuzzerTestOneInput(FuzzedDataProvider data) {
    int choice = data.consumeInt(0, 5);
    Writer writer = new FuzzWriter();
    Reader reader = new FuzzReader(data);
    Context context = new FuzzContext(data);

    try {
      switch (choice) {
        case 0:
          HandshakeResponse response =
              new HandshakeResponse(
                  credential,
                  data.consumeString(10),
                  data.consumeBytes(20),
                  conf,
                  "localhost",
                  data.consumeLong(),
                  data.consumeByte());
          response.encode(writer, context);
          break;

        case 1:
          CachingSha2PasswordPlugin sha2Plugin =
              new CachingSha2PasswordPlugin(
                  data.consumeString(20), data.consumeBytes(20), conf, hostAddress);
          sha2Plugin.process(writer, reader, context, false);
          break;

        case 2:
          NativePasswordPlugin.encryptPassword(data.consumeString(20), data.consumeBytes(20));
          break;

        case 3:
          CachingSha2PasswordPlugin.sha256encryptPassword(
              data.consumeString(20), data.consumeBytes(20));
          break;

        case 4:
          ClearPasswordPlugin clearPlugin =
              new ClearPasswordPlugin(data.consumeString(20), hostAddress, conf);
          clearPlugin.process(writer, reader, context, false);
          break;

        case 5:
          try {
            Configuration.parse("jdbc:mariadb://localhost/test?" + data.consumeString(100));
          } catch (Exception e) {
          }
          break;
      }
    } catch (Throwable e) {
      // Expected
    }
  }
}
