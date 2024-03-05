// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import com.singlestore.jdbc.ServerPreparedStatement;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.util.Parameter;
import com.singlestore.jdbc.client.util.Parameters;
import com.singlestore.jdbc.export.Prepare;
import com.singlestore.jdbc.message.ClientMessage;
import com.singlestore.jdbc.message.server.PrepareResultPacket;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

/** Execute command (COM_STMT_EXECUTE) */
public final class ExecutePacket implements RedoableWithPrepareClientMessage {
  private final String command;
  private final ServerPreparedStatement prep;
  private InputStream localInfileInputStream;
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
  public void ensureReplayable(Context context) throws IOException, SQLException {}

  /**
   * COM_STMT_EXECUTE packet
   *
   * <p>int[1] 0x17 : COM_STMT_EXECUTE header int[4] statement id int[1] flags: int[4] Iteration
   * count (always 1) if (param_count ] 0) byte[(param_count + 7)/8] null bitmap byte[1]: send type
   * to server (0 / 1) if (send type to server) for each parameter : byte[1]: field type byte[1]:
   * parameter flag for each parameter (i.e param_count times) byte[n] binary parameter value
   */
  @Override
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

      // Store types of parameters in first in first package that is sent to the server.
      for (int i = 0; i < parameterCount; i++) {
        Parameter p = parameters.get(i);
        writer.writeByte(p.getBinaryEncodeType());
        writer.writeByte(0);
        if (p.isNull()) {
          nullBitsBuffer[i / 8] |= (byte) (1 << (i % 8));
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
