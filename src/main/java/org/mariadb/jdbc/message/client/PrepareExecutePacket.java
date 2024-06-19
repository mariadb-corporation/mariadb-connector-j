// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.function.Consumer;
import org.mariadb.jdbc.BasePreparedStatement;
import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.Completion;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.ClosableLock;
import org.mariadb.jdbc.client.util.Parameter;
import org.mariadb.jdbc.client.util.Parameters;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.export.Prepare;
import org.mariadb.jdbc.message.ClientMessage;
import org.mariadb.jdbc.message.server.CachedPrepareResultPacket;
import org.mariadb.jdbc.message.server.ErrorPacket;
import org.mariadb.jdbc.message.server.PrepareResultPacket;
import org.mariadb.jdbc.plugin.codec.ByteArrayCodec;

/**
 * Send a client COM_STMT_PREPARE + COM_STMT_EXECUTE packets see
 *
 * @see <a href="https://mariadb.com/kb/en/com_stmt_prepare/">Prepare packet</a>
 */
public final class PrepareExecutePacket implements RedoableWithPrepareClientMessage {
  private final String sql;
  private final ServerPreparedStatement prep;
  private final InputStream localInfileInputStream;
  private PrepareResultPacket prepareResult;
  private Parameters parameters;

  /**
   * Construct prepare packet
   *
   * @param sql sql
   * @param parameters parameter
   * @param prep prepare
   * @param localInfileInputStream local infile input stream
   */
  public PrepareExecutePacket(
      String sql,
      Parameters parameters,
      ServerPreparedStatement prep,
      InputStream localInfileInputStream) {
    this.sql = sql;
    this.parameters = parameters;
    this.prep = prep;
    this.localInfileInputStream = localInfileInputStream;
    this.prepareResult = null;
  }

  @Override
  public int encode(Writer writer, Context context, Prepare newPrepareResult)
      throws IOException, SQLException {
    int statementId = -1;
    if (newPrepareResult == null) {

      writer.initPacket();
      writer.writeByte(0x16);
      writer.writeString(this.sql);
      writer.flushPipeline();
    } else {
      statementId = newPrepareResult.getStatementId();
    }
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
          nullBitsBuffer[i / 8] |= (byte) (1 << (i % 8));
        }
      }

      // write nullBitsBuffer in reserved place
      writer.writeBytesAtPos(nullBitsBuffer, initialPos);

      // send not null parameter, not long data
      for (int i = 0; i < parameterCount; i++) {
        Parameter p = parameters.get(i);
        if (!p.isNull() && !p.canEncodeLongData()) {
          p.encodeBinary(writer, context);
        }
      }
    }

    writer.flush();
    return (newPrepareResult == null) ? 2 : 1;
  }

  @Override
  public Completion readPacket(
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      Reader reader,
      Writer writer,
      Context context,
      ExceptionFactory exceptionFactory,
      ClosableLock lock,
      boolean traceEnable,
      ClientMessage message,
      Consumer<String> redirectFct)
      throws IOException, SQLException {
    if (this.prepareResult == null) {
      ReadableByteBuf buf = reader.readReusablePacket(traceEnable);
      // *********************************************************************************************************
      // * ERROR response
      // *********************************************************************************************************
      if (buf.getUnsignedByte()
          == 0xff) { // force current status to in transaction to ensure rollback/commit, since
        // command may
        // have issue a transaction
        ErrorPacket errorPacket = new ErrorPacket(buf, context);
        throw exceptionFactory
            .withSql(this.description())
            .create(
                errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());
      }
      if (context.getConf().useServerPrepStmts()
          && context.getConf().cachePrepStmts()
          && sql.length() < 8192) {
        PrepareResultPacket prepare = new CachedPrepareResultPacket(buf, reader, context);
        PrepareResultPacket previousCached =
            (PrepareResultPacket)
                context.putPrepareCacheCmd(
                    sql,
                    prepare,
                    stmt instanceof ServerPreparedStatement
                        ? (ServerPreparedStatement) stmt
                        : null);
        if (stmt != null) {
          ((BasePreparedStatement) stmt)
              .setPrepareResult(previousCached != null ? previousCached : prepare);
        }
        this.prepareResult = previousCached != null ? previousCached : prepare;
        return this.prepareResult;
      }
      PrepareResultPacket prepareResult = new PrepareResultPacket(buf, reader, context);
      if (stmt != null) {
        ((BasePreparedStatement) stmt).setPrepareResult(prepareResult);
      }
      this.prepareResult = prepareResult;
      return prepareResult;
    } else {
      return RedoableWithPrepareClientMessage.super.readPacket(
          stmt,
          fetchSize,
          maxRows,
          resultSetConcurrency,
          resultSetType,
          closeOnCompletion,
          reader,
          writer,
          context,
          exceptionFactory,
          lock,
          traceEnable,
          message,
          redirectFct);
    }
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

  public boolean canSkipMeta() {
    return true;
  }

  @Override
  public String description() {
    return "PREPARE + EXECUTE " + sql;
  }

  public int batchUpdateLength() {
    return 1;
  }

  public String getCommand() {
    return sql;
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

  public boolean validateLocalFileName(String fileName, Context context) {
    return ClientMessage.validateLocalFileName(sql, parameters, fileName, context);
  }

  public void setPrepareResult(PrepareResultPacket prepareResult) {
    this.prepareResult = prepareResult;
  }
}
