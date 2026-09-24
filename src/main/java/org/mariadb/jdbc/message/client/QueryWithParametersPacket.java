// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.message.client;

import static org.mariadb.jdbc.message.client.CommandConstants.COM_QUERY;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.Parameter;
import org.mariadb.jdbc.client.util.Parameters;
import org.mariadb.jdbc.message.ClientMessage;
import org.mariadb.jdbc.plugin.codec.ByteArrayCodec;
import org.mariadb.jdbc.util.ClientParser;

/**
 * Query client packet COM_QUERY see https://mariadb.com/kb/en/com_query/ same than QueryPacket, but
 * with parameters that will be escaped
 */
public final class QueryWithParametersPacket implements RedoableClientMessage {

  private final String preSqlCmd;
  private final ClientParser parser;
  private final InputStream localInfileInputStream;
  private Parameters parameters;

  /**
   * Constructor
   *
   * @param preSqlCmd additional pre command
   * @param parser command parser result
   * @param parameters parameters
   * @param localInfileInputStream local infile input stream
   */
  public QueryWithParametersPacket(
      String preSqlCmd,
      ClientParser parser,
      Parameters parameters,
      InputStream localInfileInputStream) {
    this.preSqlCmd = preSqlCmd;
    this.parser = parser;
    this.parameters = parameters;
    this.localInfileInputStream = localInfileInputStream;
  }

  @Override
  public void ensureReplayable(Context context) throws IOException, SQLException {
    int parameterCount = parameters.size();
    for (int i = 0; i < parameterCount; i++) {
      Parameter p = parameters.get(i);
      if (!p.isNull() && p.canEncodeLongData()) {
        this.parameters.set(
            i, new org.mariadb.jdbc.codec.Parameter<>(ByteArrayCodec.INSTANCE, p.encodeData()));
      }
    }
  }

  public void saveParameters() {
    this.parameters = this.parameters.clone();
  }

  @Override
  public int encode(Writer encoder, Context context) throws IOException, SQLException {
    encoder.initPacket();
    encoder.writeByte(COM_QUERY);
    if (preSqlCmd != null) encoder.writeAscii(preSqlCmd);
    if (parser.paramPositions().isEmpty()) {
      encoder.writeBytes(parser.query());
    } else {
      int pos = 0;
      int paramPos;
      for (int i = 0; i < parser.paramPositions().size(); i++) {
        paramPos = parser.paramPositions().get(i);
        encoder.writeBytes(parser.query(), pos, paramPos - pos);
        pos = paramPos + 1;
        parameters.get(i).encodeText(encoder, context);
      }
      encoder.writeBytes(parser.query(), pos, parser.query().length - pos);
    }
    encoder.flush();
    return 1;
  }

  @Override
  public int batchUpdateLength() {
    return 1;
  }

  @Override
  public boolean validateLocalFileName(String fileName, Context context) {
    return ClientMessage.validateLocalFileName(parser.sql(), parameters, fileName, context);
  }

  @Override
  public InputStream getLocalInfileInputStream() {
    return localInfileInputStream;
  }

  @Override
  public String description() {
    return parser.sql();
  }
}
