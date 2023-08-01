// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.export.Prepare;
import org.mariadb.jdbc.message.ClientMessage;

/** Client message that can be replayed */
public interface RedoableClientMessage extends ClientMessage {

  /** Save parameters of command that can be re-executed */
  default void saveParameters() {}

  /**
   * Ensure that command can be replayed
   *
   * @param context connection context
   * @throws IOException If socket error occurs
   * @throws SQLException for other type of issue
   */
  default void ensureReplayable(Context context) throws IOException, SQLException {}

  /**
   * Encode command to packet
   *
   * @param writer socket writer
   * @param context connection context
   * @param newPrepareResult new prepare result if prepare has been changed
   * @return number of send command
   * @throws IOException if any socket error is issued
   * @throws SQLException if any other kind of error occurs during encoding
   */
  default int encode(Writer writer, Context context, Prepare newPrepareResult)
      throws IOException, SQLException {
    return encode(writer, context);
  }

  /**
   * re-encode command to packet
   *
   * @param writer socket writer
   * @param context connection context
   * @param newPrepareResult new prepare result if prepare has been changed
   * @return number of send command
   * @throws IOException if any socket error is issued
   * @throws SQLException if any other kind of error occurs during encoding
   */
  default int reEncode(Writer writer, Context context, Prepare newPrepareResult)
      throws IOException, SQLException {
    return encode(writer, context, newPrepareResult);
  }
}
