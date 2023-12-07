// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

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

/** Client message that can be replayed with parameter */
public interface RedoableWithPrepareClientMessage extends RedoableClientMessage {
  /**
   * sql command value
   *
   * @return sql command
   */
  String getCommand();

  /**
   * Server prepare statement caller
   *
   * @return caller
   */
  ServerPreparedStatement prep();

  /**
   * Default encoder caller
   *
   * @param writer socket writer
   * @param context connection context
   * @return number of command sent
   * @throws IOException if a socket exception occurs
   * @throws SQLException for any other kind of error
   */
  default int encode(Writer writer, Context context) throws IOException, SQLException {
    return encode(writer, context, null);
  }

  /**
   * encoder method in case of failover, passing new prepared object
   *
   * @param writer socket writer
   * @param context connection context
   * @param newPrepareResult new prepare result
   * @return number of command sent
   * @throws IOException if a socket exception occurs
   * @throws SQLException for any other kind of error
   */
  @Override
  int encode(Writer writer, Context context, Prepare newPrepareResult)
      throws IOException, SQLException;

  /**
   * re-encoder method in case of failover, passing new prepared object
   *
   * @param writer socket writer
   * @param context connection context
   * @param newPrepareResult new prepare result
   * @return number of command sent
   * @throws IOException if a socket exception occurs
   * @throws SQLException for any other kind of error
   */
  @Override
  default int reEncode(Writer writer, Context context, Prepare newPrepareResult)
      throws IOException, SQLException {
    return encode(writer, context, newPrepareResult);
  }

  /**
   * Set prepare result, if pipelining prepare
   *
   * @param prepareResult prepare results
   */
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
