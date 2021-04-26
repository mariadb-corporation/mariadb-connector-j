// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.Parameter;
import org.mariadb.jdbc.codec.list.ByteArrayCodec;
import org.mariadb.jdbc.message.server.PrepareResultPacket;
import org.mariadb.jdbc.util.ParameterList;

/** See https://mariadb.com/kb/en/com_stmt_execute/ for documentation */
public final class ExecutePacket implements RedoableWithPrepareClientMessage {
  private ParameterList parameters;
  private final String command;
  private final ServerPreparedStatement prep;
  private PrepareResultPacket prepareResult;

  public ExecutePacket(
      PrepareResultPacket prepareResult,
      ParameterList parameters,
      String command,
      ServerPreparedStatement prep) {
    this.parameters = parameters;
    this.prepareResult = prepareResult;
    this.command = command;
    this.prep = prep;
  }

  public void saveParameters() {
    this.parameters = this.parameters.clone();
  }

  @Override
  public void ensureReplayable(Context context) throws IOException, SQLException {
    int parameterCount = parameters.size();
    for (int i = 0; i < parameterCount; i++) {
      Parameter<?> p = parameters.get(i);
      if (!p.isNull() && p.canEncodeLongData()) {
        this.parameters.set(i, new Parameter<>(ByteArrayCodec.INSTANCE, p.encodeData()));
      }
    }
  }

  public int encode(PacketWriter writer, Context context, PrepareResultPacket newPrepareResult)
      throws IOException, SQLException {

    int statementId =
        (newPrepareResult != null && newPrepareResult.getStatementId() != -1)
            ? newPrepareResult.getStatementId()
            : (this.prepareResult != null ? this.prepareResult.getStatementId() : -1);

    int parameterCount = parameters.size();

    // send long data value in separate packet
    for (int i = 0; i < parameterCount; i++) {
      Parameter<?> p = parameters.get(i);
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
        Parameter<?> p = parameters.get(i);
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
        Parameter<?> p = parameters.get(i);
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

  public ServerPreparedStatement prep() {
    return prep;
  }

  public boolean binaryProtocol() {
    return true;
  }

  public String description() {
    return command;
  }

  public void setPrepareResult(PrepareResultPacket prepareResult) {
    this.prepareResult = prepareResult;
  }
}
