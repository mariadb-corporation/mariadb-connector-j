// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.mariadb.jdbc.BasePreparedStatement;
import org.mariadb.jdbc.client.Client;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.export.Prepare;
import org.mariadb.jdbc.message.server.PrepareResultPacket;

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
  BasePreparedStatement prep();

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

  /**
   * Force re-prepare command
   *
   * @param client client
   * @throws SQLException if any error occurs
   */
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
