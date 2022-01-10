// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.client.Client;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.export.Prepare;
import org.mariadb.jdbc.message.server.PrepareResultPacket;

public interface RedoableWithPrepareClientMessage extends RedoableClientMessage {
  String getCommand();

  ServerPreparedStatement prep();

  default int encode(Writer writer, Context context) throws IOException, SQLException {
    return encode(writer, context, null);
  }

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
                    false,
                    true)
                .get(0));
  }
}
