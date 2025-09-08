// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.Parameter;
import org.mariadb.jdbc.client.util.Parameters;
import org.mariadb.jdbc.plugin.codec.ByteArrayCodec;
import org.mariadb.jdbc.util.ClientParser;

/**
 * batch execution using REWRITE.
 *
 * @see <a href="https://mariadb.com/kb/en/com_stmt_bulk_execute/">documentation</a>
 */
public final class QueryWithParametersRewritePacket implements RedoableClientMessage {
  private final String preSqlCmd;
  private final ClientParser parser;
  private List<Parameters> parametersList;

  /**
   * @param preSqlCmd pre sql command
   * @param parser parser
   * @param batchParameterList parameters
   */
  public QueryWithParametersRewritePacket(
      String preSqlCmd, ClientParser parser, List<Parameters> batchParameterList) {
    this.preSqlCmd = preSqlCmd;
    this.parametersList = batchParameterList;
    this.parser = parser;
  }

  public void saveParameters() {
    List<Parameters> savedList = new ArrayList<>(parametersList.size());
    for (Parameters parameterList : parametersList) {
      savedList.add(parameterList.clone());
    }
    this.parametersList = savedList;
  }

  @Override
  public void ensureReplayable(Context context) throws IOException, SQLException {
    for (int j = 0; j < parametersList.size(); j++) {
      Parameters parameters = parametersList.get(j);
      int parameterCount = parameters.size();
      for (int i = 0; i < parameterCount; i++) {
        Parameter p = parameters.get(i);
        if (!p.isNull() && p.canEncodeLongData()) {
          parameters.set(
              i, new org.mariadb.jdbc.codec.Parameter<>(ByteArrayCodec.INSTANCE, p.encodeData()));
        }
      }
    }
  }

  public int encode(Writer writer, Context context) throws IOException, SQLException {

    Iterator<Parameters> paramIterator = parametersList.iterator();
    Parameters parameters = paramIterator.next();

    int rewritePacketNo = 0;
    int endingPartLen = parser.getQuery().length - parser.getValuesBracketPositions().get(1);

    // Implementation After writing a bunch of parameter to buffer is marked. then : - when writing
    // next bunch of parameter, if buffer grow more than max_allowed_packet, send buffer up to mark,
    // then create a new packet with current bunch of data - if a bunch of parameter data type
    // changes
    // send buffer up to mark, then create a new packet with new data type.
    // Problem remains if a bunch of parameter is bigger than max_allowed_packet
    main_loop:
    while (true) {
      rewritePacketNo++;

      writer.initPacket();
      writer.writeByte(0x03);
      if (preSqlCmd != null) writer.writeAscii(preSqlCmd);

      int pos = 0;
      int paramPos;
      if (parser.getParamCount() > parameters.size()) {
        throw context.getExceptionFactory().create("wrong number of parameters", "Y0000");
      }

      for (int i = 0; i < parser.getParamCount(); i++) {
        paramPos = parser.getParamPositions().get(i);
        writer.writeBytes(parser.getQuery(), pos, paramPos - pos);
        pos = paramPos + 1;
        parameters.get(i).encodeText(writer, context);
      }

      if (paramIterator.hasNext()) {
        parameters = paramIterator.next();
      } else break;

      if (writer.throwMaxAllowedLengthOr16M(writer.pos() + endingPartLen)) {
        writer.writeBytes(
            parser.getQuery(), parser.getValuesBracketPositions().get(1), endingPartLen);
        writer.flush();
        continue;
      }

      parameter_loop:
      while (true) {

        // check packet length so to separate in multiple packet
        int parameterLength = 0;
        boolean knownParameterSize = true;
        if (parser.getParamCount() > parameters.size()) {
          throw context.getExceptionFactory().create("wrong number of parameters", "Y0000");
        }
        for (int i = 0; i < parser.getParamCount(); i++) {
          int paramSize = parameters.get(i).getApproximateTextProtocolLength();
          if (paramSize == -1) {
            knownParameterSize = false;
            break;
          }
          if (i > 0) {
            parameterLength +=
                parser.getParamPositions().get(i) - (parser.getParamPositions().get(i - 1) + 1);
          }
          parameterLength += paramSize;
        }

        if (!knownParameterSize
            || writer.throwMaxAllowedLengthOr16M(writer.pos() + parameterLength)) {
          writer.writeBytes(
              parser.getQuery(), parser.getValuesBracketPositions().get(1), endingPartLen);
          writer.flush();
          break;
        }

        writer.writeBytes(
            parser.getQuery(), pos, parser.getValuesBracketPositions().get(1) + 1 - pos);
        writer.writeByte((byte) ',');

        pos = parser.getValuesBracketPositions().get(0);
        for (int i = 0; i < parser.getParamPositions().size(); i++) {
          paramPos = parser.getParamPositions().get(i);
          writer.writeBytes(parser.getQuery(), pos, paramPos - pos);
          pos = paramPos + 1;
          parameters.get(i).encodeText(writer, context);
        }

        if (paramIterator.hasNext()) {
          parameters = paramIterator.next();
        } else break main_loop;
      }
    }
    writer.writeBytes(parser.getQuery(), parser.getValuesBracketPositions().get(1), endingPartLen);
    writer.flush();

    return rewritePacketNo;
  }

  public boolean binaryProtocol() {
    return false;
  }

  @Override
  public InputStream getLocalInfileInputStream() {
    return null;
  }

  public String description() {
    return "REWRITE: " + preSqlCmd + parser.getSql();
  }

  @Override
  public int batchUpdateLength() {
    return parametersList.size();
  }

  @Override
  public boolean validateLocalFileName(String fileName, Context context) {
    return false;
  }
}
