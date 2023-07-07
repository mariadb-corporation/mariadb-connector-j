// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.util.Parameter;
import com.singlestore.jdbc.client.util.Parameters;
import com.singlestore.jdbc.plugin.codec.ByteArrayCodec;
import com.singlestore.jdbc.util.ClientParser;
import java.io.IOException;
import java.sql.SQLException;

public final class QueryWithParametersPacket implements RedoableClientMessage {

  private final String preSqlCmd;
  private final ClientParser parser;
  private Parameters parameters;

  public QueryWithParametersPacket(String preSqlCmd, ClientParser parser, Parameters parameters) {
    this.preSqlCmd = preSqlCmd;
    this.parser = parser;
    this.parameters = parameters;
  }

  @Override
  public void ensureReplayable(Context context) throws IOException, SQLException {
    int parameterCount = parameters.size();
    for (int i = 0; i < parameterCount; i++) {
      Parameter p = parameters.get(i);
      if (!p.isNull() && p.canEncodeLongData()) {
        this.parameters.set(
            i, new com.singlestore.jdbc.codec.Parameter<>(ByteArrayCodec.INSTANCE, p.encodeData()));
      }
    }
  }

  public void saveParameters() {
    this.parameters = this.parameters.clone();
  }

  @Override
  public int encode(Writer encoder, Context context) throws IOException, SQLException {
    encoder.initPacket();
    encoder.writeByte(0x03);
    if (!preSqlCmd.isEmpty()) encoder.writeAscii(preSqlCmd);
    if (parser.getParamCount() == 0) {
      encoder.writeBytes(parser.getQueryParts().get(0));
    } else {
      encoder.writeBytes(parser.getQueryParts().get(0));
      for (int i = 0; i < parser.getParamCount(); i++) {
        if (parameters.get(i).isNull()) {
          encoder.writeAscii("null");
        } else {
          parameters.get(i).encodeText(encoder, context);
        }
        encoder.writeBytes(parser.getQueryParts().get(i + 1));
      }
    }
    encoder.flush();
    return 1;
  }

  public int batchUpdateLength() {
    return 1;
  }

  @Override
  public String description() {
    return parser.getSql();
  }
}
