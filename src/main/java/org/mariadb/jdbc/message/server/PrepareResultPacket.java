// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.message.server;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.BasePreparedStatement;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.impl.readable.BufferedReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.export.Prepare;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

/**
 * Prepare result packet
 *
 * @see <a href="https://mariadb.com/kb/en/com_stmt_prepare/#COM_STMT_PREPARE_OK">Prepare result
 *     packet</a>
 */
public class PrepareResultPacket implements Completion, Prepare {
  static final ColumnDecoder CONSTANT_PARAMETER;
  private static final Logger logger = Loggers.getLogger(PrepareResultPacket.class);

  static {
    byte[] bytes =
        new byte[] {
          0x03,
          0x64,
          0x65,
          0x66,
          0x00,
          0x00,
          0x00,
          0x01,
          0x3F,
          0x00,
          0x00,
          0x0C,
          0x3F,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x06,
          (byte) 0x80,
          0x00,
          0x00,
          0x00,
          0x00
        };
    CONSTANT_PARAMETER = ColumnDecoder.decode(new BufferedReadableByteBuf(bytes, bytes.length));
  }

  private final ColumnDecoder[] parameters;

  /** prepare statement id */
  protected int statementId;

  private ColumnDecoder[] columns;

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
            context
                .getColumnDecoderFunction()
                .apply((BufferedReadableByteBuf) reader.readPacket(trace, false));
      }
      if (!context.isEofDeprecated()) {
        reader.skipPacket();
      }
    }
    if (numColumns > 0) {
      for (int i = 0; i < numColumns; i++) {
        columns[i] =
            context
                .getColumnDecoderFunction()
                .apply((BufferedReadableByteBuf) reader.readPacket(trace, false));
      }
      if (!context.isEofDeprecated()) {
        reader.skipPacket();
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
  public void decrementUse(Client con, BasePreparedStatement preparedStatement)
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
