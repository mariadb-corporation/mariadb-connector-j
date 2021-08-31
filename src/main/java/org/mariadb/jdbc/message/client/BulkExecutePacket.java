// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.Parameter;
import org.mariadb.jdbc.client.util.Parameters;
import org.mariadb.jdbc.export.MaxAllowedPacketException;
import org.mariadb.jdbc.export.Prepare;
import org.mariadb.jdbc.message.server.PrepareResultPacket;

public final class BulkExecutePacket implements RedoableWithPrepareClientMessage {
  private List<Parameters> batchParameterList;
  private final String command;
  private final ServerPreparedStatement prep;
  private Prepare prepareResult;

  public BulkExecutePacket(
      Prepare prepareResult,
      List<Parameters> batchParameterList,
      String command,
      ServerPreparedStatement prep) {
    this.batchParameterList = batchParameterList;
    this.prepareResult = prepareResult;
    this.command = command;
    this.prep = prep;
  }

  public void saveParameters() {
    List<Parameters> savedList = new ArrayList<>(batchParameterList.size());
    for (Parameters parameterList : batchParameterList) {
      savedList.add(parameterList.clone());
    }
    this.batchParameterList = savedList;
  }

  public int encode(Writer writer, Context context, Prepare newPrepareResult)
      throws IOException, SQLException {

    int statementId =
        (newPrepareResult != null && newPrepareResult.getStatementId() != -1)
            ? newPrepareResult.getStatementId()
            : (this.prepareResult != null ? this.prepareResult.getStatementId() : -1);

    Iterator<Parameters> paramIterator = batchParameterList.iterator();
    Parameters parameters = paramIterator.next();
    int parameterCount = parameters.size();

    @SuppressWarnings("rawtypes")
    Parameter[] parameterHeaderType = new Parameter[parameterCount];
    // set header type
    for (int i = 0; i < parameterCount; i++) {
      parameterHeaderType[i] = parameters.get(i);
    }
    byte[] lastCmdData = null;
    int bulkPacketNo = 0;

    // Implementation After writing a bunch of parameter to buffer is marked. then : - when writing
    // next bunch of parameter, if buffer grow more than max_allowed_packet, send buffer up to mark,
    // then create a new packet with current bunch of data - if a bunch of parameter data type
    // changes
    // send buffer up to mark, then create a new packet with new data type.
    // Problem remains if a bunch of parameter is bigger than max_allowed_packet
    main_loop:
    while (true) {
      bulkPacketNo++;

      writer.initPacket();
      writer.writeByte(0xfa); // COM_STMT_BULK_EXECUTE
      writer.writeInt(statementId);
      writer.writeShort((short) 128); // always SEND_TYPES_TO_SERVER

      for (int i = 0; i < parameterCount; i++) {
        writer.writeShort((short) parameterHeaderType[i].getBinaryEncodeType());
      }

      if (lastCmdData != null) {
        if (writer.throwMaxAllowedLength(lastCmdData.length)) {
          throw new MaxAllowedPacketException(
              "query size is >= to max_allowed_packet", writer.getCmdLength() != 0);
        }
        writer.writeBytes(lastCmdData);
        writer.mark();
        lastCmdData = null;
        if (!paramIterator.hasNext()) {
          break;
        }
        parameters = paramIterator.next();
      }

      parameter_loop:
      while (true) {
        for (int i = 0; i < parameterCount; i++) {
          Parameter param = parameters.get(i);
          if (param.isNull()) {
            writer.writeByte(0x01); // value is null
          } else {
            writer.writeByte(0x00); // value follow
            param.encodeBinary(writer);
          }
        }

        if (!writer.isMarked() && writer.hasFlushed()) {
          // parameter were too big to fit in a MySQL packet
          // need to finish the packet separately
          writer.flush();
          if (!paramIterator.hasNext()) {
            break main_loop;
          }
          parameters = paramIterator.next();
          // reset header type
          for (int j = 0; j < parameterCount; j++) {
            parameterHeaderType[j] = parameters.get(j);
          }
          break;
        }

        writer.mark();

        if (writer.bufIsDataAfterMark()) {
          // flush has been done
          lastCmdData = writer.resetMark();
          break;
        }

        if (!paramIterator.hasNext()) {
          break main_loop;
        }

        parameters = paramIterator.next();

        // ensure type has not changed
        for (int i = 0; i < parameterCount; i++) {
          if (parameterHeaderType[i].getBinaryEncodeType()
              != parameters.get(i).getBinaryEncodeType()) {
            writer.flush();
            // reset header type
            for (int j = 0; j < parameterCount; j++) {
              parameterHeaderType[j] = parameters.get(j);
            }
            break parameter_loop;
          }
        }
      }
    }

    writer.flush();

    return bulkPacketNo;
  }

  public int batchUpdateLength() {
    return batchParameterList.size();
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
