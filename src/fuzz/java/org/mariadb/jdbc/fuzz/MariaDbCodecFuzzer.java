// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab

package org.mariadb.jdbc.fuzz;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Calendar;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.fuzz.support.FuzzColumn;
import org.mariadb.jdbc.fuzz.support.FuzzContext;
import org.mariadb.jdbc.fuzz.support.FuzzValueGenerator;
import org.mariadb.jdbc.fuzz.support.FuzzWriter;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.plugin.codec.*;

/** Professional fuzzer for MariaDB data codecs (all 36+ types). */
public class MariaDbCodecFuzzer {

  public static void fuzzerTestOneInput(FuzzedDataProvider data) {
    Codec<?> codec = data.pickValue(FuzzValueGenerator.CODECS);
    FuzzWriter writer = new FuzzWriter();
    Context context = new FuzzContext(data);
    Calendar cal = Calendar.getInstance();

    try {
      int action = data.consumeInt(0, 3);
      switch (action) {
        case 0: // Fuzz Decoding (Text)
          fuzzDecode(data, codec, context, cal, true);
          break;
        case 1: // Fuzz Decoding (Binary)
          fuzzDecode(data, codec, context, cal, false);
          break;
        case 2: // Fuzz Encoding (Text)
          fuzzEncode(data, codec, writer, context, cal, true);
          break;
        case 3: // Fuzz Encoding (Binary)
          fuzzEncode(data, codec, writer, context, cal, false);
          break;
      }
    } catch (IOException | SQLException | RuntimeException e) {
      // Expected
    }
  }

  private static void fuzzDecode(
      FuzzedDataProvider data, Codec<?> codec, Context context, Calendar cal, boolean text)
      throws SQLException {
    byte[] raw = data.consumeBytes(data.consumeInt(0, 1024));
    ReadableByteBuf buffer = new StandardReadableByteBuf(raw);
    MutableInt length = new MutableInt(raw.length);
    ColumnDecoder column = new FuzzColumn();

    if (text) {
      codec.decodeText(buffer, length, column, cal, context);
    } else {
      codec.decodeBinary(buffer, length, column, cal, context);
    }
  }

  private static void fuzzEncode(
      FuzzedDataProvider data,
      Codec<?> codec,
      Writer writer,
      Context context,
      Calendar cal,
      boolean text)
      throws IOException, SQLException {
    Object value = FuzzValueGenerator.generate(data, codec);
    if (value == null) return;

    if (text) {
      codec.encodeText(writer, context, value, cal, data.consumeLong());
    } else {
      codec.encodeBinary(writer, context, value, cal, data.consumeLong());
    }
  }

}
