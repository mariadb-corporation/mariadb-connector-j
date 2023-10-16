// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.Parameter;
import org.mariadb.jdbc.client.util.Parameters;
import org.mariadb.jdbc.export.Prepare;
import org.mariadb.jdbc.message.ClientMessage;
import org.mariadb.jdbc.message.server.PrepareResultPacket;
import org.mariadb.jdbc.plugin.codec.ByteArrayCodec;

/**
 * Execute command (COM_STMT_EXECUTE)
 *
 * @see <a href="https://mariadb.com/kb/en/com_stmt_execute/">Execute documentation</a>
 */
public final class ExecutePacket implements RedoableWithPrepareClientMessage {
  private final String command;
  private final ServerPreparedStatement prep;
  private final InputStream localInfileInputStream;
  private Prepare prepareResult;
  private Parameters parameters;

  /**
   * Constructor
   *
   * @param prepareResult prepare result
   * @param parameters parameter
   * @param command sql command
   * @param prep prepared statement
   * @param localInfileInputStream local infile input stream
   */
  public ExecutePacket(
      Prepare prepareResult,
      Parameters parameters,
      String command,
      ServerPreparedStatement prep,
      InputStream localInfileInputStream) {
    this.parameters = parameters;
    this.prepareResult = prepareResult;
    this.command = command;
    this.prep = prep;
    this.localInfileInputStream = localInfileInputStream;
  }

  public void saveParameters() {
    this.parameters = this.parameters.clone();
  }

  @Override
  public void ensureReplayable(Context context) throws IOException, SQLException {
    int parameterCount = parameters.size();
    for (int i = 0; i < parameterCount; i++) {
      Parameter p = parameters.get(i);
      if (!p.isNull() && p.canEncodeLongData()) {
        this.parameters.set(
            i, new org.mariadb.jdbc.codec.Parameter<>(ByteArrayCodec.INSTANCE, p.encodeData()));
      }
    }
  }

  public int encode(Writer writer, Context context, Prepare newPrepareResult)
      throws IOException, SQLException {

    int statementId =
        (newPrepareResult != null && newPrepareResult.getStatementId() != -1)
            ? newPrepareResult.getStatementId()
            : (this.prepareResult != null ? this.prepareResult.getStatementId() : -1);

    int parameterCount = parameters.size();

    // send long data value in separate packet
    for (int i = 0; i < parameterCount; i++) {
      Parameter p = parameters.get(i);
      if (!p.isNull() && p.canEncodeLongData()) {
        new LongDataPacket(statementId, p, i).encode(writer, context);
      }
    }

    writer.initPacket();
    writer.writeByte(0x17);
    writer.writeInt(statementId);
    writer.writeByte(0x00); // NO CURSOR
    writer.writeInt(1); // Iteration pos

    if (parameterCount > 0) {

      // create null bitmap and reserve place in writer
      int nullCount = (parameterCount + 7) / 8;
      byte[] nullBitsBuffer = new byte[nullCount];
      int initialPos = writer.pos();
      writer.pos(initialPos + nullCount);

      // Send Parameter type flag
      writer.writeByte(0x01);

      // Store types of parameters in first package that is sent to the server.
      for (int i = 0; i < parameterCount; i++) {
        Parameter p = parameters.get(i);
        writer.writeByte(p.getBinaryEncodeType());
        writer.writeByte(0);
        if (p.isNull()) {
          nullBitsBuffer[i / 8] |= (1 << (i % 8));
        }
      }

      // write nullBitsBuffer in reserved place
      writer.writeBytesAtPos(nullBitsBuffer, initialPos);

      // send not null parameter, not long data
      for (int i = 0; i < parameterCount; i++) {
        Parameter p = parameters.get(i);
        if (!p.isNull() && !p.canEncodeLongData()) {
          p.encodeBinary(writer);
        }
      }
    }

    writer.flush();
    return 1;
  }

  public boolean canSkipMeta() {
    return true;
  }

  public int batchUpdateLength() {
    return 1;
  }

  public String getCommand() {
    return command;
  }

  public InputStream getLocalInfileInputStream() {
    return localInfileInputStream;
  }

  public ServerPreparedStatement prep() {
    return prep;
  }

  public boolean binaryProtocol() {
    return true;
  }

  public String description() {
    return "EXECUTE " + command;
  }

  public boolean validateLocalFileName(String fileName, Context context) {
    return ClientMessage.validateLocalFileName(command, parameters, fileName, context);
  }

  public void setPrepareResult(PrepareResultPacket prepareResult) {
    this.prepareResult = prepareResult;
  }
}
