/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.util.ClientParser;
import org.mariadb.jdbc.util.ParameterList;

public final class QueryWithParametersPacket implements RedoableClientMessage {

  private final String preSqlCmd;
  private final ClientParser parser;
  private ParameterList parameters;

  public QueryWithParametersPacket(
      String preSqlCmd, ClientParser parser, ParameterList parameters) {
    this.preSqlCmd = preSqlCmd;
    this.parser = parser;
    this.parameters = parameters;
  }

  public void saveParameters() {
    this.parameters = this.parameters.clone();
  }

  @Override
  public int encode(PacketWriter encoder, Context context) throws IOException, SQLException {
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
