// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2021-2024 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.util.Parameter;
import com.singlestore.jdbc.client.util.Parameters;
import com.singlestore.jdbc.plugin.codec.ByteArrayCodec;
import com.singlestore.jdbc.util.ClientParser;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Query with parameters packet used for insert batch with rewrite option {@link
 * Configuration#rewriteBatchedStatements()}
 */
public class RewriteQueryMultiPacket implements RedoableClientMessage {

  private static final int PARAMETER_LENGTH = 1024;
  private final ClientParser parser;
  private Parameters parameters;
  private final int paramCount;

  /**
   * Constructor
   *
   * @param paramCount original query param count
   * @param parser command parser result
   * @param parameters parameters
   */
  public RewriteQueryMultiPacket(int paramCount, ClientParser parser, Parameters parameters) {
    this.paramCount = paramCount;
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
    int packetsSize = 0;
    encoder.initPacket();
    encoder.writeByte(0x03);
    if (parser.getParamPositions().size() == 0) {
      encoder.writeBytes(parser.getQuery());
    } else {
      int pos = 0;
      int lastParamPos = parser.getParamPositions().get(parser.getParamPositions().size() - 1) + 1;
      int paramPosIdx = 0;
      int paramPos;
      for (int i = 0; i < parser.getParamPositions().size(); i++) {
        if (encoder.throwMaxAllowedLength(encoder.pos() + paramCount * PARAMETER_LENGTH)
            && paramPosIdx != 0
            && paramPosIdx % paramCount == 0) {
          encoder.writeBytes(
              parser.getQuery(), lastParamPos, parser.getQuery().length - lastParamPos);
          encoder.flush();
          packetsSize++;
          encoder.initPacket();
          encoder.writeByte(0x03);
          pos = 0;
          paramPosIdx = 0;
        }
        paramPos = parser.getParamPositions().get(paramPosIdx);
        encoder.writeBytes(parser.getQuery(), pos, paramPos - pos);
        pos = paramPos + 1;
        parameters.get(i).encodeText(encoder, context);
        paramPosIdx++;
      }
      encoder.writeBytes(parser.getQuery(), lastParamPos, parser.getQuery().length - lastParamPos);
    }
    encoder.flush();
    packetsSize++;
    return packetsSize;
  }

  public int batchUpdateLength() {
    return 1;
  }

  @Override
  public String description() {
    return parser.getSql();
  }
}
