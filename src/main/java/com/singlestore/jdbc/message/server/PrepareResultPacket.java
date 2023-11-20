// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.server;

import com.singlestore.jdbc.ServerPreparedStatement;
import com.singlestore.jdbc.client.Client;
import com.singlestore.jdbc.client.ColumnDecoder;
import com.singlestore.jdbc.client.Completion;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.socket.Reader;
import com.singlestore.jdbc.export.Prepare;
import com.singlestore.jdbc.util.constants.Capabilities;
import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;
import java.io.IOException;
import java.sql.SQLException;

public class PrepareResultPacket implements Completion, Prepare {
  private final ColumnDecoder[] parameters;
  private ColumnDecoder[] columns;
  /** prepare statement id */
  protected int statementId;

  /**
   * Prepare packet constructor (parsing)
   *
   * @param buffer packet buffer
   * @param reader packet reader
   * @param context connection context
   * @throws IOException if socket exception occurs
   */
  public PrepareResultPacket(ReadableByteBuf buffer, Reader reader, Context context)
      throws IOException {
    Logger logger = Loggers.getLogger(PrepareResultPacket.class);
    boolean trace = logger.isTraceEnabled();
    buffer.readByte(); /* skip COM_STMT_PREPARE_OK */
    this.statementId = buffer.readInt();
    final int numColumns = buffer.readUnsignedShort();
    final int numParams = buffer.readUnsignedShort();
    this.parameters = new ColumnDecoder[numParams];
    this.columns = new ColumnDecoder[numColumns];
    if (numParams > 0) {
      for (int i = 0; i < numParams; i++) {
        parameters[i] =
            ColumnDecoder.decode(
                reader.readPacket(false, trace),
                (context.getServerCapabilities() & Capabilities.EXTENDED_TYPE_INFO) > 0);
      }
      if (!context.isEofDeprecated()) {
        reader.readPacket(true, trace);
      }
    }
    if (numColumns > 0) {
      for (int i = 0; i < numColumns; i++) {
        columns[i] =
            ColumnDecoder.decode(
                reader.readPacket(false, trace),
                (context.getServerCapabilities() & Capabilities.EXTENDED_TYPE_INFO) > 0);
      }
      if (!context.isEofDeprecated()) {
        reader.readPacket(true, trace);
      }
    }
  }

  /**
   * Close prepare packet
   *
   * @param con current connection
   * @throws SQLException if exception occurs
   */
  public void close(Client con) throws SQLException {
    con.closePrepare(this);
  }

  /**
   * Decrement use of prepare packet, so closing it if last used
   *
   * @param con connection
   * @param preparedStatement current prepared statement that was using prepare object
   * @throws SQLException if exception occurs
   */
  public void decrementUse(Client con, ServerPreparedStatement preparedStatement)
      throws SQLException {
    close(con);
  }

  /**
   * Get statement id
   *
   * @return statement id
   */
  public int getStatementId() {
    return statementId;
  }

  public ColumnDecoder[] getParameters() {
    return parameters;
  }

  public ColumnDecoder[] getColumns() {
    return columns;
  }

  public void setColumns(ColumnDecoder[] columns) {
    this.columns = columns;
  }
}
