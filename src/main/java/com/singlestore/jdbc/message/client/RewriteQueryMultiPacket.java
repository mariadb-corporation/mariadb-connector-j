// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2021-2024 SingleStore, Inc.

package com.singlestore.jdbc.message.client;

import static org.mariadb.jdbc.internal.util.SqlStates.INTERRUPTED_EXCEPTION;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.util.Parameter;
import com.singlestore.jdbc.client.util.Parameters;
import com.singlestore.jdbc.util.RewriteClientParser;
import com.singlestore.jdbc.util.log.Loggers;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Query with parameters packet used for insert batch with rewrite option {@link
 * Configuration#rewriteBatchedStatements()}
 */
public class RewriteQueryMultiPacket implements RedoableClientMessage {

  private final Configuration config;
  private final RewriteClientParser parser;
  private List<Parameters> batchParameters;
  private final int paramCount;

  /**
   * Constructor
   *
   * @param paramCount original query param count
   * @param parser command parser result
   * @param batchParameters batch parameters
   */
  public RewriteQueryMultiPacket(
      Configuration config,
      int paramCount,
      RewriteClientParser parser,
      List<Parameters> batchParameters) {
    this.config = config;
    this.paramCount = paramCount;
    this.parser = parser;
    this.batchParameters = batchParameters;
  }

  @Override
  public void ensureReplayable(Context context) {}

  public void saveParameters() {
    List<Parameters> clonedBatches = new ArrayList<>();
    for (Parameters parameters : batchParameters) {
      clonedBatches.add(parameters.clone());
    }
    this.batchParameters = clonedBatches;
  }

  @Override
  public int encode(Writer encoder, Context context) throws IOException, SQLException {
    int packetsSize = 0;
    int currentIndex = 0;
    int totalParameterList = batchParameters.size();
    do {
      currentIndex = sendRewriteCmd(encoder, context, currentIndex);
      packetsSize++; // number of flushes
      if (Thread.currentThread().isInterrupted()) {
        throw new SQLException("Interrupted during batch", INTERRUPTED_EXCEPTION.getSqlState(), -1);
      }

    } while (currentIndex < totalParameterList);
    return packetsSize;
  }

  /**
   * Client side PreparedStatement.executeBatch values rewritten (concatenate value params according
   * to max_allowed_packet)
   *
   * @param currentIndex currentIndex
   * @return current index
   * @throws IOException if connection fail
   */
  private int sendRewriteCmd(Writer encoder, Context context, int currentIndex)
      throws IOException, SQLException {
    int batchIndex = currentIndex;

    encoder.initPacket();
    encoder.writeByte(0x03);
    Parameters parameters = batchParameters.get(batchIndex++);
    List<byte[]> queryParts = parser.getQueryParts();

    byte[] firstPart = queryParts.get(0);
    byte[] secondPart = queryParts.get(1);

    encoder.writeBytes(firstPart, 0, firstPart.length);
    encoder.writeBytes(secondPart, 0, secondPart.length);

    int packetLength = parser.getQueryPartsLength() + getApproximateParametersLength(parameters);

    for (int i = 0; i < paramCount; i++) {
      parameters.get(i).encodeText(encoder, context);
      encoder.writeBytes(queryParts.get(i + 2));
    }

    if (parser.isQueryMultiValuesRewritable()) {
      Loggers.getLogger(RewriteQueryMultiPacket.class)
          .debug("execute multi values rewrite batch query");
      while (batchIndex < batchParameters.size()) {
        parameters = batchParameters.get(batchIndex);

        // check packet length so to separate in multiple packet
        packetLength += getApproximateParametersLength(parameters);
        if (!encoder.throwMaxAllowedLength(packetLength)) {
          encoder.writeByte((byte) ',');
          encoder.writeBytes(secondPart, 0, secondPart.length);

          for (int i = 0; i < paramCount; i++) {
            parameters.get(i).encodeText(encoder, context);
            byte[] addPart = queryParts.get(i + 2);
            encoder.writeBytes(addPart, 0, addPart.length);
          }
          batchIndex++;
        } else {
          Loggers.getLogger(RewriteQueryMultiPacket.class)
              .debug(
                  "split multi values rewrite batch query on {} batch with size {}",
                  batchIndex,
                  packetLength);
          break;
        }
      }
      encoder.writeBytes(queryParts.get(paramCount + 2));
    } else {
      encoder.writeBytes(queryParts.get(paramCount + 2));
      while (batchIndex < batchParameters.size()) {
        if (!config.allowMultiQueries()) {
          break;
        }
        parameters = batchParameters.get(batchIndex);
        // check packet length so to separate in multiple packet
        packetLength +=
            getApproximateParametersLength(parameters) + parser.getQueryPartsLength() + 1;
        if (!encoder.throwMaxAllowedLength(packetLength)) {
          encoder.writeByte((byte) ';');
          encoder.writeBytes(firstPart, 0, firstPart.length);
          encoder.writeBytes(secondPart, 0, secondPart.length);
          for (int i = 0; i < paramCount; i++) {
            parameters.get(i).encodeText(encoder, context);
            encoder.writeBytes(queryParts.get(i + 2));
          }
          encoder.writeBytes(queryParts.get(paramCount + 2));
          batchIndex++;
        } else {
          Loggers.getLogger(RewriteQueryMultiPacket.class)
              .debug(
                  "split  multi queries command on {} batch with size {}",
                  batchIndex,
                  packetLength);
          break;
        }
      }
    }
    encoder.flush();
    return batchIndex;
  }

  private int getApproximateParametersLength(Parameters parameters)
      throws IOException, SQLException {
    int parameterLength = 0;
    for (int i = 0; i < paramCount; i++) {
      Parameter parameter = parameters.get(i);
      int paramSize = parameter.getApproximateTextProtocolLength();
      if (paramSize == -1) {
        return 1024; // default approximate
      }
      parameterLength += paramSize;
    }
    return parameterLength;
  }

  public int batchUpdateLength() {
    return 1;
  }

  @Override
  public String description() {
    return parser.getSql();
  }
}
