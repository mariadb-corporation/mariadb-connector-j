// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.ServerPreparedStatement;
import com.singlestore.jdbc.client.Client;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.export.Prepare;
import com.singlestore.jdbc.message.server.PrepareResultPacket;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface RedoableWithPrepareClientMessage extends RedoableClientMessage {
  String getCommand();

  ServerPreparedStatement prep();

  default int encode(Writer writer, Context context) throws IOException, SQLException {
    return encode(writer, context, null);
  }

  @Override
  int encode(Writer writer, Context context, Prepare newPrepareResult)
      throws IOException, SQLException;

  @Override
  default int reEncode(Writer writer, Context context, Prepare newPrepareResult)
      throws IOException, SQLException {
    return encode(writer, context, newPrepareResult);
  }

  void setPrepareResult(PrepareResultPacket prepareResult);

  default void rePrepare(Client client) throws SQLException {
    PreparePacket preparePacket = new PreparePacket(getCommand());
    setPrepareResult(
        (PrepareResultPacket)
            client
                .execute(
                    preparePacket,
                    prep(),
                    0,
                    0L,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.TYPE_FORWARD_ONLY,
                    false)
                .get(0));
  }
}
