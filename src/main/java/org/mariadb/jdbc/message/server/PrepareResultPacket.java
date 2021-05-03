// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.server;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketReader;
import org.mariadb.jdbc.util.constants.Capabilities;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

/** See https://mariadb.com/kb/en/com_stmt_prepare/#COM_STMT_PREPARE_OK */
public class PrepareResultPacket implements Completion {
  private static final Logger logger = Loggers.getLogger(PrepareResultPacket.class);
  private final ColumnDefinitionPacket[] parameters;
  private ColumnDefinitionPacket[] columns;
  protected int statementId;

  public PrepareResultPacket(ReadableByteBuf buffer, PacketReader reader, Context context)
      throws IOException {
    boolean trace = logger.isTraceEnabled();
    buffer.readByte(); /* skip COM_STMT_PREPARE_OK */
    this.statementId = buffer.readInt();
    final int numColumns = buffer.readUnsignedShort();
    final int numParams = buffer.readUnsignedShort();
    this.parameters = new ColumnDefinitionPacket[numParams];
    this.columns = new ColumnDefinitionPacket[numColumns];
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

  public ColumnDefinitionPacket[] getParameters() {
    return parameters;
  }

  public ColumnDefinitionPacket[] getColumns() {
    return columns;
  }

  public void setColumns(ColumnDefinitionPacket[] columns) {
    this.columns = columns;
  }
}
