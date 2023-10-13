// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.export.Prepare;
import com.singlestore.jdbc.message.ClientMessage;
import java.io.IOException;
import java.sql.SQLException;

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
