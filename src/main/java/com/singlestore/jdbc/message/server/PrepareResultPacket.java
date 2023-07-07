// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.server;

import com.singlestore.jdbc.ServerPreparedStatement;
import com.singlestore.jdbc.client.Client;
import com.singlestore.jdbc.client.Column;
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
  private final Column[] parameters;
  private Column[] columns;
  protected int statementId;

  public PrepareResultPacket(ReadableByteBuf buffer, Reader reader, Context context)
      throws IOException {
    Logger logger = Loggers.getLogger(PrepareResultPacket.class);
    boolean trace = logger.isTraceEnabled();
    buffer.readByte(); /* skip COM_STMT_PREPARE_OK */
    this.statementId = buffer.readInt();
    final int numColumns = buffer.readUnsignedShort();
    final int numParams = buffer.readUnsignedShort();
    this.parameters = new Column[numParams];
    this.columns = new Column[numColumns];
    if (numParams > 0) {
      for (int i = 0; i < numParams; i++) {
        parameters[i] =
            new ColumnDefinitionPacket(
                reader.readPacket(false, trace),
                (context.getServerCapabilities() & Capabilities.MARIADB_CLIENT_EXTENDED_TYPE_INFO)
                    > 0);
      }
      if (!context.isEofDeprecated()) {
        reader.readPacket(true, trace);
      }
    }
    if (numColumns > 0) {
      for (int i = 0; i < numColumns; i++) {
        columns[i] =
            new ColumnDefinitionPacket(
                reader.readPacket(false, trace),
                (context.getServerCapabilities() & Capabilities.MARIADB_CLIENT_EXTENDED_TYPE_INFO)
                    > 0);
      }
      if (!context.isEofDeprecated()) {
        reader.readPacket(true, trace);
      }
    }
  }

  public void close(Client con) throws SQLException {
    con.closePrepare(this);
  }

  public void decrementUse(Client con, ServerPreparedStatement preparedStatement)
      throws SQLException {
    close(con);
  }

  public int getStatementId() {
    return statementId;
  }

  public Column[] getParameters() {
    return parameters;
  }

  public Column[] getColumns() {
    return columns;
  }

  public void setColumns(Column[] columns) {
    this.columns = columns;
  }
}
